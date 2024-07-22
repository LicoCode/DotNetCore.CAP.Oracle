// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Oracle;
using DotNetCore.CAP.Persistence;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace DotNetCore.CAP.Oracle;

public class OracleStorageInitializer : IStorageInitializer
{
    private readonly IOptions<CapOptions> _capOptions;
    private readonly ILogger _logger;
    private readonly IOptions<OracleOptions> _options;

    public OracleStorageInitializer(
        ILogger<OracleStorageInitializer> logger,
        IOptions<OracleOptions> options, IOptions<CapOptions> capOptions)
    {
        _capOptions = capOptions;
        _options = options;
        _logger = logger;
    }

    public virtual string GetPublishedTableName()
    {
        return $"{_options.Value.TablePrefix.ToUpper()}_PUBLISHED";
    }

    public virtual string GetReceivedTableName()
    {
        return $"{_options.Value.TablePrefix.ToUpper()}_RECEIVED";
    }

    public virtual string GetLockTableName()
    {
        return $"{_options.Value.TablePrefix.ToUpper()}_LOCK";
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested) return;

        var sql = CreateDbTablesScript();
        var connection = new OracleConnection(_options.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(sql).ConfigureAwait(false);

        if (_capOptions.Value.UseStorageLock) {
            sql = InitLockDbTableScript();
            object[] sqlParams =
            {
                new OracleParameter("PubKey", $"publish_retry_{_capOptions.Value.Version}"),
                new OracleParameter("LastLockTime", DateTime.MinValue) { OracleDbType = OracleDbType.Date },
                new OracleParameter("RecKey", $"received_retry_{_capOptions.Value.Version}"),
                
            };
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
        }
        _logger.LogDebug("Ensuring all create database tables script are applied.");
    }

    protected virtual string CreateDbTablesScript()
    {
        var batchSql =
            $@"
DECLARE  
    v_count NUMBER;  
BEGIN  
    SELECT COUNT(*)  
    INTO v_count  
    FROM user_tables  
    WHERE table_name = '{GetReceivedTableName()}';  
    IF v_count = 0 THEN  
        EXECUTE IMMEDIATE 'CREATE TABLE {GetReceivedTableName()} (  
            ID                NUMBER(20) NOT NULL,  
            VERSION           NVARCHAR2(20) NOT NULL,  
            NAME              NVARCHAR2(200) NOT NULL,  
            ""GROUP""         NVARCHAR2(200),  
            CONTENT           NVARCHAR2(2000),  
            RETRIES           NUMBER(10) NOT NULL,  
            ADDED             DATE NOT NULL,  
            EXPIRESAT         DATE,  
            STATUSNAME        NVARCHAR2(50) NOT NULL,  
            CONSTRAINT PK_{GetReceivedTableName()} PRIMARY KEY (ID)  
        )';  
    END IF; 
    SELECT COUNT(*)  
    INTO v_count  
    FROM user_tables  
    WHERE table_name = '{GetPublishedTableName()}';  
    IF v_count = 0 THEN  
        EXECUTE IMMEDIATE 'CREATE TABLE {GetPublishedTableName()} (  
            ID                NUMBER(20) NOT NULL,  
            VERSION           NVARCHAR2(20) NOT NULL,  
            NAME              NVARCHAR2(200) NOT NULL,   
            CONTENT           NVARCHAR2(2000),  
            RETRIES           NUMBER(10) NOT NULL,  
            ADDED             DATE NOT NULL,  
            EXPIRESAT         DATE,  
            STATUSNAME        NVARCHAR2(50) NOT NULL,  
            CONSTRAINT PK_{GetPublishedTableName()} PRIMARY KEY (ID)  
        )';  
    END IF;
    ";
        if (_capOptions.Value.UseStorageLock)
            batchSql += $@"
    SELECT COUNT(*)  
    INTO v_count  
    FROM user_tables  
    WHERE table_name = '{GetLockTableName()}';  
    IF v_count = 0 THEN  
        EXECUTE IMMEDIATE 'CREATE TABLE ""{GetLockTableName()}"" (  
            KEY                NVARCHAR2(128) NOT NULL,  
            INSTANCE           NVARCHAR2(256),  
            LASTLOCKTIME       DATE,   
            CONSTRAINT PK_{GetLockTableName()} PRIMARY KEY (KEY)  
        )';  
    END IF; ";
        batchSql += $@"
END;";
        return batchSql;
    }

    protected virtual string InitLockDbTableScript() {
        return @$"
DECLARE  
    v_count NUMBER;  
BEGIN  
    SELECT COUNT(*)  
    INTO v_count  
    FROM ""{GetLockTableName()}"";  
    IF v_count = 0 THEN  
        INSERT ALL INTO ""{GetLockTableName()}"" (KEY,INSTANCE,LASTLOCKTIME) VALUES(:PubKey,'',:LastLockTime) 
        INTO ""{GetLockTableName()}"" (KEY,INSTANCE,LASTLOCKTIME) VALUES(:RecKey,'',:LastLockTime) SELECT 1 FROM DUAL;
    END IF;
END;";
    }
}