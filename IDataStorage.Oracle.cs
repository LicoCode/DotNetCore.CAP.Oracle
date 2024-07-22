// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;

namespace DotNetCore.CAP.Oracle;

public class OracleDataStorage : IDataStorage
{
    private readonly IOptions<CapOptions> _capOptions;
    private readonly IStorageInitializer _initializer;
    private readonly string _lockName;
    private readonly IOptions<OracleOptions> _options;
    private readonly string _pubName;
    private readonly string _recName;
    private readonly ISerializer _serializer;
    private readonly ISnowflakeId _snowflakeId;

    public OracleDataStorage(
        IOptions<CapOptions> capOptions,
        IOptions<OracleOptions> options,
        IStorageInitializer initializer,
        ISerializer serializer,
        ISnowflakeId snowflakeId)
    {
        _options = options;
        _initializer = initializer;
        _capOptions = capOptions;
        _serializer = serializer;
        _snowflakeId = snowflakeId;
        _pubName = initializer.GetPublishedTableName();
        _recName = initializer.GetReceivedTableName();
        _lockName = initializer.GetLockTableName();
    }

    public async Task<bool> AcquireLockAsync(string key, TimeSpan ttl, string instance,
        CancellationToken token = default)
    {
        var sql =
            @$"UPDATE ""{_lockName}"" SET INSTANCE=:Instance,LASTLOCKTIME=:LastLockTime WHERE KEY=:Key AND LASTLOCKTIME < :TTL";
        var connection = new OracleConnection(_options.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        object[] sqlParams =
        {
            new OracleParameter("Instance", instance),
            new OracleParameter("LastLockTime", DateTime.Now){ OracleDbType = OracleDbType.Date},
            new OracleParameter("Key", key),
            new OracleParameter("TTL", DateTime.Now.Subtract(ttl)){ OracleDbType = OracleDbType.Date}
        };
        var opResult = await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
        return opResult > 0;
    }

    public async Task ReleaseLockAsync(string key, string instance, CancellationToken cancellationToken = default)
    {
        var sql =
            @$"UPDATE ""{_lockName}"" SET INSTANCE='',LASTLOCKTIME=:LastLockTime WHERE KEY=:Key AND INSTANCE=:Instance";
        var connection = new OracleConnection(_options.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        object[] sqlParams =
        {
            
            new OracleParameter("LastLockTime", DateTime.MinValue) { OracleDbType = OracleDbType.Date },
            new OracleParameter("Key", key),
            new OracleParameter("Instance", instance),
        };
        await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
    }

    public async Task RenewLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default)
    {
        var sql =
            @$"UPDATE ""{_lockName}"" SET LASTLOCKTIME= LASTLOCKTIME + ({ttl.TotalSeconds} / (24 * 60 * 60)) WHERE KEY=:Key AND INSTANCE=:Instance";
        var connection = new OracleConnection(_options.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        object[] sqlParams =
        {
            new OracleParameter("Key", key),
            new OracleParameter("Instance", instance)
        };
        await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
    }

    public async Task ChangePublishStateToDelayedAsync(string[] ids)
    {
        var sql = $"UPDATE {_pubName} SET STATUSNAME='{StatusName.Delayed}' WHERE ID IN ({string.Join(',', ids)});";
        var connection = new OracleConnection(_options.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(sql).ConfigureAwait(false);
    }

    public async Task ChangePublishStateAsync(MediumMessage message, StatusName state, object? transaction = null)
    {
        await ChangeMessageStateAsync(_pubName, message, state, transaction).ConfigureAwait(false);
    }

    public async Task ChangeReceiveStateAsync(MediumMessage message, StatusName state)
    {
        await ChangeMessageStateAsync(_recName, message, state).ConfigureAwait(false);
    }

    public async Task<MediumMessage> StoreMessageAsync(string name, Message content, object? transaction = null)
    {
        var sql =
            $"INSERT INTO {_pubName} (ID,VERSION,NAME,CONTENT,RETRIES,ADDED,EXPIRESAT,STATUSNAME)" +
            $"VALUES(:Id,'{_options.Value.Version}',:Name,:Content,:Retries,:Added,:ExpiresAt,:StatusName)";

        var message = new MediumMessage
        {
            DbId = content.GetId(),
            Origin = content,
            Content = _serializer.Serialize(content),
            Added = DateTime.Now,
            ExpiresAt = null,
            Retries = 0
        };

        object[] sqlParams =
        {
            new OracleParameter("Id", message.DbId),
            new OracleParameter("Name", name),
            new OracleParameter("Content", message.Content),
            new OracleParameter("Retries", message.Retries),
            new OracleParameter("Added", message.Added),
            new OracleParameter("ExpiresAt", message.ExpiresAt.HasValue ? message.ExpiresAt.Value : DBNull.Value),
            new OracleParameter("StatusName", nameof(StatusName.Scheduled))
        };

        if (transaction == null)
        {
            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
        }
        else
        {
            var dbTrans = transaction as DbTransaction;
            if (dbTrans == null && transaction is IDbContextTransaction dbContextTrans)
                dbTrans = dbContextTrans.GetDbTransaction();

            var conn = dbTrans?.Connection;
            await conn!.ExecuteNonQueryAsync(sql, dbTrans, sqlParams).ConfigureAwait(false);
        }

        return message;
    }

    public async Task StoreReceivedExceptionMessageAsync(string name, string group, string content)
    {
        object[] sqlParams =
        {
            new OracleParameter("Id", _snowflakeId.NextId().ToString()),
            new OracleParameter("Name", name),
            new OracleParameter("OGroup", group),
            new OracleParameter("Content", content),
            new OracleParameter("Retries", _capOptions.Value.FailedRetryCount),
            new OracleParameter("Added", DateTime.Now),
            new OracleParameter("ExpiresAt", DateTime.Now.AddSeconds(_capOptions.Value.FailedMessageExpiredAfter)),
            new OracleParameter("StatusName", nameof(StatusName.Failed))
        };

        await StoreReceivedMessage(sqlParams).ConfigureAwait(false);
    }

    public async Task<MediumMessage> StoreReceivedMessageAsync(string name, string group, Message message)
    {
        var mdMessage = new MediumMessage
        {
            DbId = _snowflakeId.NextId().ToString(),
            Origin = message,
            Added = DateTime.Now,
            ExpiresAt = null,
            Retries = 0
        };

        object[] sqlParams =
        {
            new OracleParameter("Id", mdMessage.DbId),
            new OracleParameter("Name", name),
            new OracleParameter("OGroup", group),
            new OracleParameter("Content", _serializer.Serialize(mdMessage.Origin)),
            new OracleParameter("Retries", mdMessage.Retries),
            new OracleParameter("Added", mdMessage.Added){ OracleDbType = OracleDbType.Date},
            new OracleParameter("ExpiresAt", mdMessage.ExpiresAt.HasValue ? mdMessage.ExpiresAt.Value : DBNull.Value){ OracleDbType = OracleDbType.Date},
            new OracleParameter("StatusName", nameof(StatusName.Scheduled))
        };

        await StoreReceivedMessage(sqlParams).ConfigureAwait(false);

        return mdMessage;
    }

    public async Task<int> DeleteExpiresAsync(string table, DateTime timeout, int batchCount = 1000,
        CancellationToken token = default)
    {
        var connection = new OracleConnection(_options.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        var sql = $"DELETE FROM {table} WHERE EXPIRESAT < :timeout AND (STATUSNAME='{StatusName.Succeeded}' OR STATUSNAME='{StatusName.Failed}') AND ROWNUM <= :batchCount";
        return await connection.ExecuteNonQueryAsync(sql,null,
            new OracleParameter("timeout", timeout) { OracleDbType = OracleDbType.Date }, new OracleParameter("batchCount", batchCount)).ConfigureAwait(false);
    }

    public Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
    {
        return GetMessagesOfNeedRetryAsync(_pubName, lookbackSeconds);
    }

    public Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
    {
        return GetMessagesOfNeedRetryAsync(_recName, lookbackSeconds);
    }

    public async Task ScheduleMessagesOfDelayedAsync(Func<object, IEnumerable<MediumMessage>, Task> scheduleTask,
        CancellationToken token = default)
    {
        var sql =
            $"SELECT ID,CONTENT,RETRIES,ADDED,EXPIRESAT FROM {_pubName} WHERE VERSION=:Version " +
            $"AND ((EXPIRESAT< :TwoMinutesLater AND STATUSNAME = '{StatusName.Delayed}') OR (EXPIRESAT< :OneMinutesAgo AND STATUSNAME = '{StatusName.Queued}'))";

        object[] sqlParams =
        {
            new OracleParameter("Version", _capOptions.Value.Version),
            new OracleParameter("TwoMinutesLater", DateTime.Now.AddMinutes(2)),
            new OracleParameter("OneMinutesAgo", DateTime.Now.AddMinutes(-1))
        };

        await using var connection = new OracleConnection(_options.Value.ConnectionString);
        await connection.OpenAsync(token);
        await using var transaction = await connection.BeginTransactionAsync(token);
        var messageList = await connection.ExecuteReaderAsync(sql, async reader =>
        {
            var messages = new List<MediumMessage>();
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                messages.Add(new MediumMessage
                {
                    DbId = reader.GetInt64(0).ToString(),
                    Origin = _serializer.Deserialize(reader.GetString(1))!,
                    Retries = reader.GetInt32(2),
                    Added = reader.GetDateTime(3),
                    ExpiresAt = reader.GetDateTime(4)
                });
            }

            return messages;
        }, transaction, sqlParams).ConfigureAwait(false);

        await scheduleTask(transaction, messageList);

        await transaction.CommitAsync(token);
    }

    public IMonitoringApi GetMonitoringApi()
    {
        return new OracleMonitoringApi(_options, _initializer, _serializer);
    }

    private async Task ChangeMessageStateAsync(string tableName, MediumMessage message, StatusName state,
        object? transaction = null)
    {
        var sql =
            @$"UPDATE {tableName} SET CONTENT=:Content, RETRIES=:Retries,EXPIRESAT =:ExpiresAt ,  STATUSNAME=:StatusName WHERE ID =:Id";

        object[] sqlParams =
        {
            new OracleParameter("Content", _serializer.Serialize(message.Origin)),
            new OracleParameter("Retries", message.Retries),
            new OracleParameter("ExpiresAt",  message.ExpiresAt.HasValue ? message.ExpiresAt.Value : DBNull.Value){ OracleDbType = OracleDbType.Date},
            new OracleParameter("StatusName", state.ToString()),
            new OracleParameter("Id", Int64.Parse(message.DbId)){ OracleDbType = OracleDbType.Int64},
        };

        if (transaction is DbTransaction dbTransaction)
        {
            var connection = (OracleConnection)dbTransaction.Connection!;
            await connection.ExecuteNonQueryAsync(sql, dbTransaction, sqlParams).ConfigureAwait(false);
        }
        else
        {
            var connection = new OracleConnection(_options.Value.ConnectionString);
            await using var _ = connection.ConfigureAwait(false);
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
        }
    }

    private async Task StoreReceivedMessage(object[] sqlParams)
    {
        var sql =
            $@"INSERT INTO {_recName}(ID,VERSION,NAME,""GROUP"",CONTENT,RETRIES,ADDED,EXPIRESAT,STATUSNAME)" +
            $" VALUES(:Id,'{_capOptions.Value.Version}',:Name,:OGroup,:Content,:Retries,:Added,:ExpiresAt,:StatusName)";

        var connection = new OracleConnection(_options.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
    }

    private async Task<IEnumerable<MediumMessage>> GetMessagesOfNeedRetryAsync(string tableName, TimeSpan lookbackSeconds)
    {
        var fourMinAgo = DateTime.Now.Subtract(lookbackSeconds);
        var sql =
            $"SELECT ID, CONTENT, RETRIES, ADDED FROM (SELECT ID, CONTENT, RETRIES, ADDED, ROWNUM  FROM {tableName} WHERE RETRIES<:Retries " +
            $"AND VERSION=:Version AND ADDED<:Added AND (STATUSNAME = '{StatusName.Failed}' OR STATUSNAME = '{StatusName.Scheduled}')) where ROWNUM <= 200";

        object[] sqlParams =
        {
            new OracleParameter("Retries", _capOptions.Value.FailedRetryCount),
            new OracleParameter("Version", _capOptions.Value.Version),
            new OracleParameter("Added", fourMinAgo)
        };

        var connection = new OracleConnection(_options.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        var result = await connection.ExecuteReaderAsync(sql, async reader =>
        {
            var messages = new List<MediumMessage>();
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                messages.Add(new MediumMessage
                {
                    DbId = reader.GetInt64(0).ToString(),
                    Origin = _serializer.Deserialize(reader.GetString(1))!,
                    Retries = reader.GetInt32(2),
                    Added = reader.GetDateTime(3)
                });
            }

            return messages;
        }, sqlParams: sqlParams).ConfigureAwait(false);

        return result;
    }
}