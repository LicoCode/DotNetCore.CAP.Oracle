// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;

namespace DotNetCore.CAP.Oracle;

internal class OracleMonitoringApi : IMonitoringApi
{
    private readonly OracleOptions _options;
    private readonly string _pubName;
    private readonly string _recName;
    private readonly ISerializer _serializer;

    public OracleMonitoringApi(IOptions<OracleOptions> options, IStorageInitializer initializer,
        ISerializer serializer)
    {
        _options = options.Value ?? throw new ArgumentNullException(nameof(options));
        _pubName = initializer.GetPublishedTableName();
        _recName = initializer.GetReceivedTableName();
        _serializer = serializer;
    }

    public async Task<StatisticsDto> GetStatisticsAsync()
    {
        var sql = $@"
SELECT
(
    SELECT COUNT(ID) FROM {_pubName} WHERE STATUSNAME = N'Succeeded'
) AS PublishedSucceeded,
(
    SELECT COUNT(ID) FROM {_recName} WHERE STATUSNAME = N'Succeeded'
) AS ReceivedSucceeded,
(
    SELECT COUNT(ID) FROM {_pubName} WHERE STATUSNAME = N'Failed'
) AS PublishedFailed,
(
    SELECT COUNT(ID) FROM {_recName} WHERE STATUSNAME = N'Failed'
) AS ReceivedFailed,
(
    SELECT COUNT(ID) FROM {_pubName} WHERE STATUSNAME = N'Delayed'
) AS PublishedDelayed FROM DUAL";

        var connection = new OracleConnection(_options.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        var statistics = await connection.ExecuteReaderAsync(sql, reader =>
        {
            var statisticsDto = new StatisticsDto();

            while (reader.Read())
            {
                statisticsDto.PublishedSucceeded = reader.GetInt32(0);
                statisticsDto.ReceivedSucceeded = reader.GetInt32(1);
                statisticsDto.PublishedFailed = reader.GetInt32(2);
                statisticsDto.ReceivedFailed = reader.GetInt32(3);
                statisticsDto.PublishedDelayed = reader.GetInt32(4);
            }

            return Task.FromResult(statisticsDto);
        }).ConfigureAwait(false);

        return statistics;
    }

    public async Task<IDictionary<DateTime, int>> HourlyFailedJobs(MessageType type)
    {
        var tableName = type == MessageType.Publish ? _pubName : _recName;
        return await GetHourlyTimelineStats(tableName, nameof(StatusName.Failed)).ConfigureAwait(false);
    }

    public async Task<IDictionary<DateTime, int>> HourlySucceededJobs(MessageType type)
    {
        var tableName = type == MessageType.Publish ? _pubName : _recName;
        return await GetHourlyTimelineStats(tableName, nameof(StatusName.Succeeded)).ConfigureAwait(false);
    }

    public async Task<PagedQueryResult<MessageDto>> GetMessagesAsync(MessageQueryDto queryDto)
    {
        var tableName = queryDto.MessageType == MessageType.Publish ? _pubName : _recName;
        var where = string.Empty;
        var sqlallparams = new List<Object>();
        var sqlparams = new List<Object>();
        if (!string.IsNullOrEmpty(queryDto.StatusName)) {
            where += " AND p.STATUSNAME=:StatusName";
            sqlparams.Add(new OracleParameter("StatusName", GetStatusName(queryDto.StatusName).ToString()));
            sqlallparams.Add(new OracleParameter("StatusName", GetStatusName(queryDto.StatusName).ToString()));
        }
        if (!string.IsNullOrEmpty(queryDto.Name)) {
            where += " AND p.NAME=:Name";
            sqlparams.Add(new OracleParameter("Name", queryDto.Name ?? string.Empty));
            sqlallparams.Add(new OracleParameter("Name", queryDto.Name ?? string.Empty));
        }
        if (!string.IsNullOrEmpty(queryDto.Group)) {
            where += @" AND p.""GROUP""=:OGroup";
            sqlparams.Add(new OracleParameter("OGroup", queryDto.Group ?? string.Empty));
            sqlallparams.Add(new OracleParameter("OGroup", queryDto.Group ?? string.Empty));
        }
        if (!string.IsNullOrEmpty(queryDto.Content)) {
            where += " AND p.CONTENT LIKE :Content";
            sqlparams.Add(new OracleParameter("Content", $"%{queryDto.Content}%"));
            sqlallparams.Add(new OracleParameter("Content", $"%{queryDto.Content}%"));
        }
        var pagesql = " AND ROWNUM <= (:CurrentPage + 1) * :PageSize ";
        var sqlQuery =
            $"SELECT rw.* FROM(SELECT p.*, ROWNUM as rn FROM {tableName} p WHERE 1=1 {where}{pagesql} ORDER BY p.ADDED DESC) rw WHERE rw.rn >= :CurrentPage * :PageSize + 1 ";

        object[] sqlPageParams = 
        {
            new OracleParameter("CurrentPage", queryDto.CurrentPage),
            new OracleParameter("PageSize", queryDto.PageSize)
        };
        sqlallparams.AddRange(sqlPageParams);
        var connection = new OracleConnection(_options.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);

        var count = await connection.ExecuteScalarAsync<int>($"SELECT COUNT(1) FROM {tableName} p WHERE 1=1 {where}",
            sqlparams.ToArray()).ConfigureAwait(false);

        var items = await connection.ExecuteReaderAsync(sqlQuery,
            async reader =>
            {
                var messages = new List<MessageDto>();

                while (await reader.ReadAsync().ConfigureAwait(false))
                {
                    var index = 0;
                    messages.Add(new MessageDto
                    {
                        Id = reader.GetInt64(index++).ToString(),
                        Version = reader.GetString(index++),
                        Name = reader.GetString(index++),
                        Group = queryDto.MessageType == MessageType.Subscribe ? reader.GetString(index++) : default,
                        Content = reader.GetString(index++),
                        Retries = reader.GetInt32(index++),
                        Added = reader.GetDateTime(index++),
                        ExpiresAt = reader.IsDBNull(index++) ? null : reader.GetDateTime(index - 1),
                        StatusName = reader.GetString(index)
                    });
                }

                return messages;
            }, sqlParams: sqlallparams.ToArray()).ConfigureAwait(false);

        return new PagedQueryResult<MessageDto>
            { Items = items, PageIndex = queryDto.CurrentPage, PageSize = queryDto.PageSize, Totals = count };
    }

    public StatusName GetStatusName(string status) {
        StatusName? statusName = Enum.GetValues(typeof(StatusName))
    .Cast<StatusName>()
    .FirstOrDefault(c => c.ToString().Equals(status, StringComparison.OrdinalIgnoreCase));
        return statusName.Value;
    }

    public ValueTask<int> PublishedFailedCount()
    {
        return GetNumberOfMessage(_pubName, nameof(StatusName.Failed));
    }

    public ValueTask<int> PublishedSucceededCount()
    {
        return GetNumberOfMessage(_pubName, nameof(StatusName.Succeeded));
    }

    public ValueTask<int> ReceivedFailedCount()
    {
        return GetNumberOfMessage(_recName, nameof(StatusName.Failed));
    }

    public ValueTask<int> ReceivedSucceededCount()
    {
        return GetNumberOfMessage(_recName, nameof(StatusName.Succeeded));
    }

    public async Task<MediumMessage?> GetPublishedMessageAsync(long id)
    {
        return await GetMessageAsync(_pubName, id).ConfigureAwait(false);
    }

    public async Task<MediumMessage?> GetReceivedMessageAsync(long id)
    {
        return await GetMessageAsync(_recName, id).ConfigureAwait(false);
    }

    private async ValueTask<int> GetNumberOfMessage(string tableName, string statusName)
    {
        var sqlQuery =
            $"select count(ID) from {tableName}  where STATUSNAME = :state";
        var connection = new OracleConnection(_options.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        return await connection.ExecuteScalarAsync<int>(sqlQuery, new OracleParameter("state", statusName))
            .ConfigureAwait(false);
    }

    private Task<Dictionary<DateTime, int>> GetHourlyTimelineStats(string tableName, string statusName)
    {
        var endDate = DateTime.Now;
        var dates = new List<DateTime>();
        for (var i = 0; i < 24; i++)
        {
            dates.Add(endDate);
            endDate = endDate.AddHours(-1);
        }

        var keyMaps = dates.ToDictionary(x => x.ToString("yyyy-MM-dd-HH"), x => x);

        return GetTimelineStats(tableName, statusName, keyMaps);
    }

    private async Task<Dictionary<DateTime, int>> GetTimelineStats(
        string tableName,
        string statusName,
        IDictionary<string, DateTime> keyMaps)
    {
        var sqlQuery = $@"
with aggr as (
    select TO_CHAR(ADDED,'yyyy-MM-dd-HH') as Key,
        count(ID) Count
    from  {tableName}
    where STATUSNAME = :statusName
    group by TO_CHAR(ADDED,'yyyy-MM-dd-HH')
)
select Key, Count from aggr where Key BETWEEN  :minKey and :maxKey";

        object[] sqlParams =
        {
            new OracleParameter("statusName", statusName),
            new OracleParameter("minKey", keyMaps.Keys.Min()),
            new OracleParameter("maxKey", keyMaps.Keys.Max())
        };

        Dictionary<string, int> valuesMap;
        var connection = new OracleConnection(_options.ConnectionString);
        await using (connection.ConfigureAwait(false))
        {
            valuesMap = await connection.ExecuteReaderAsync(sqlQuery,
                async reader =>
                {
                    var dictionary = new Dictionary<string, int>();

                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        dictionary.Add(reader.GetString(0), reader.GetInt32(1));
                    }

                    return dictionary;
                }, sqlParams: sqlParams).ConfigureAwait(false);
        }

        foreach (var key in keyMaps.Keys)
        {
            valuesMap.TryAdd(key, 0);
        }

        var result = new Dictionary<DateTime, int>();
        for (var i = 0; i < keyMaps.Count; i++)
        {
            var value = valuesMap[keyMaps.ElementAt(i).Key];
            result.Add(keyMaps.ElementAt(i).Value, value);
        }

        return result;
    }

    private async Task<MediumMessage?> GetMessageAsync(string tableName, long id)
    {
        var sql =
            $@"SELECT ID AS DbId, CONTENT, ADDED, EXPIRESAT, RETRIES FROM {tableName} WHERE Id={id} AND ROWNUM = 1";

        var connection = new OracleConnection(_options.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        var mediumMessage = await connection.ExecuteReaderAsync(sql, async reader =>
        {
            MediumMessage? message = null;

            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                message = new MediumMessage
                {
                    DbId = reader.GetInt64(0).ToString(),
                    Origin = _serializer.Deserialize(reader.GetString(1))!,
                    Content = reader.GetString(1),
                    Added = reader.GetDateTime(2),
                    ExpiresAt = reader.GetDateTime(3),
                    Retries = reader.GetInt32(4)
                };
            }

            return message;
        }).ConfigureAwait(false);

        return mediumMessage;
    }
}