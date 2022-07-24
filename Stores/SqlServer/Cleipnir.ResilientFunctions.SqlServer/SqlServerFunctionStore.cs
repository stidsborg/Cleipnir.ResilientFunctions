using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerFunctionStore : IFunctionStore
{
    private readonly Func<Task<SqlConnection>> _connFunc;
    private readonly string _tablePrefix;

    public SqlServerFunctionStore(string connectionString, string tablePrefix = "")
        : this(CreateConnection(connectionString), tablePrefix) {}
    
    public SqlServerFunctionStore(Func<Task<SqlConnection>> connFunc, string tablePrefix = "")
    {
        _connFunc = connFunc;
        _tablePrefix = tablePrefix;
    }

    private static Func<Task<SqlConnection>> CreateConnection(string connectionString)
    {
        return async () =>
        {
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        };
    }

    public async Task Initialize()
    {
        await using var conn = await _connFunc();
        var sql = @$"
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{_tablePrefix}RFunctions' and xtype='U')
            BEGIN
                CREATE TABLE {_tablePrefix}RFunctions (
                    FunctionTypeId NVARCHAR(200) NOT NULL,
                    FunctionInstanceId NVARCHAR(200) NOT NULL,
                    ParamJson NVARCHAR(MAX) NULL,
                    ParamType NVARCHAR(255) NULL,
                    ScrapbookJson NVARCHAR(MAX) NULL,
                    ScrapbookType NVARCHAR(255) NULL,
                    Status INT NOT NULL,
                    ResultJson NVARCHAR(MAX) NULL,
                    ResultType NVARCHAR(255) NULL,
                    ErrorJson NVARCHAR(MAX) NULL,
                    PostponedUntil BIGINT NULL,
                    Epoch INT NOT NULL,
                    SignOfLife INT NOT NULL,
                    PRIMARY KEY (FunctionTypeId, FunctionInstanceId)
                );
                CREATE INDEX {_tablePrefix}RFunctions_idx_Executing
                  ON {_tablePrefix}RFunctions (FunctionTypeId, FunctionInstanceId)
                  INCLUDE (Epoch, SignOfLife)
                  WHERE Status = {(int)Status.Executing};
                CREATE INDEX {_tablePrefix}RFunctions_idx_Postponed
                  ON {_tablePrefix}RFunctions (FunctionTypeId, PostponedUntil, FunctionInstanceId)
                  INCLUDE (Epoch)
                  WHERE Status = {(int)Status.Postponed};
            END
                
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[{_tablePrefix}RFunctions]') AND name = 'CrashedCheckFrequency')
                ALTER TABLE {_tablePrefix}RFunctions ADD [CrashedCheckFrequency] BIGINT DEFAULT 0 NOT NULL;
            IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[{_tablePrefix}RFunctions]') AND name = 'Version')
                ALTER TABLE {_tablePrefix}RFunctions ADD [Version] INT DEFAULT 0 NOT NULL;";

        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropIfExists()
    {
        await using var conn = await _connFunc();
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}RFunctions";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Truncate()
    {
        await using var conn = await _connFunc();
        var sql = $"TRUNCATE TABLE {_tablePrefix}RFunctions";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> CreateFunction(
        FunctionId functionId,
        StoredParameter param,
        string? scrapbookType,
        long crashedCheckFrequency,
        int version
    )
    {
        await using var conn = await _connFunc();
        try
        {
            var sql = @$"
                INSERT INTO {_tablePrefix}RFunctions(
                    FunctionTypeId, FunctionInstanceId, 
                    ParamJson, ParamType, 
                    ScrapbookType, 
                    Status,
                    Epoch, SignOfLife, 
                    CrashedCheckFrequency,
                    Version)
                VALUES(
                    @FunctionTypeId, @FunctionInstanceId, 
                    @ParamJson, @ParamType,  
                    @ScrapbookType,
                    {(int) Status.Executing},
                    0, 0,
                    @CrashedCheckFrequency,
                    @Version)";
            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@ParamJson", param.ParamJson);
            command.Parameters.AddWithValue("@ParamType", param.ParamType);
            command.Parameters.AddWithValue("@ScrapbookType", scrapbookType ?? (object) DBNull.Value);
            command.Parameters.AddWithValue("@CrashedCheckFrequency", crashedCheckFrequency);
            command.Parameters.AddWithValue("@Version", version);

            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException sqlException)
        {
            if (sqlException.Number == SqlError.UNIQUENESS_VIOLATION) return false;
        }

        return true;
    }

    public async Task<bool> TryToBecomeLeader(
        FunctionId functionId, 
        Status newStatus, 
        int expectedEpoch, 
        int newEpoch, 
        long crashedCheckFrequency,
        int version)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Epoch = @NewEpoch, Status = @NewStatus, CrashedCheckFrequency = @CrashedCheckFrequency, Version = @Version
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@NewEpoch", newEpoch);
        command.Parameters.AddWithValue("@NewStatus", newStatus);
        command.Parameters.AddWithValue("@CrashedCheckFrequency", crashedCheckFrequency);
        command.Parameters.AddWithValue("@Version", version);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET SignOfLife = @SignOfLife
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @Epoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@SignOfLife", newSignOfLife);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@Epoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT FunctionInstanceId, Epoch, SignOfLife, CrashedCheckFrequency
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId AND Status = {(int) Status.Executing}";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionTypeId.Value);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<StoredExecutingFunction>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var functionInstanceId = reader.GetString(0);
                var epoch = reader.GetInt32(1);
                var signOfLife = reader.GetInt32(2);
                var crashedCheckFrequency = reader.GetInt64(3);
                rows.Add(new StoredExecutingFunction(functionInstanceId, epoch, signOfLife, crashedCheckFrequency));    
            }

            reader.NextResult();
        }

        return rows;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {

        await using var conn = await _connFunc();
        var sql = @$"
            SELECT FunctionInstanceId, Epoch, PostponedUntil
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId AND Status = {(int) Status.Postponed} AND PostponedUntil <= @PostponedUntil";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionTypeId.Value);
        command.Parameters.AddWithValue("@PostponedUntil", expiresBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<StoredPostponedFunction>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var functionInstanceId = reader.GetString(0);
                var epoch = reader.GetInt32(1);
                var postponedUntil = reader.GetInt64(2);
                rows.Add(new StoredPostponedFunction(functionInstanceId, epoch, postponedUntil));    
            }

            reader.NextResult();
        }

        return rows;
    }

    public async Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string? scrapbookJson,
        StoredResult? result,
        string? errorJson,
        long? postponedUntil,
        int expectedEpoch
    )
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET
                Status = @Status,
                ScrapbookJson = @ScrapbookJson,
                ResultJson = @ResultJson, ResultType = @ResultType,
                ErrorJson = @ErrorJson,
                PostponedUntil = @PostponedUntil
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@Status", (int) status);
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ResultJson", result?.ResultJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ResultType", result?.ResultType ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ErrorJson", errorJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@PostponedUntil", postponedUntil ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT  ParamJson, ParamType,
                    ScrapbookJson, ScrapbookType,
                    Status,
                    ResultJson, ResultType,
                    ErrorJson,
                    PostponedUntil,
                    Epoch, SignOfLife
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var paramJson = reader.GetString(0);
                var paramType = reader.GetString(1);
                var scrapbookJson = reader.IsDBNull(2) ? null : reader.GetString(2);
                var scrapbookType = reader.IsDBNull(3) ? null : reader.GetString(3);
                var status = (Status) reader.GetInt32(4);
                var resultJson = reader.IsDBNull(5) ? null : reader.GetString(5);
                var resultType = reader.IsDBNull(6) ? null : reader.GetString(6);
                var errorJson = reader.IsDBNull(7) ? null : reader.GetString(7);
                var postponedUntil = reader.IsDBNull(8) ? default(long?) : reader.GetInt64(8);
                var epoch = reader.GetInt32(9);
                var signOfLife = reader.GetInt32(10);

                return new StoredFunction(
                    functionId,
                    new StoredParameter(paramJson, paramType),
                    scrapbookType == null ? null : new StoredScrapbook(scrapbookJson, scrapbookType),
                    status,
                    resultType == null ? null : new StoredResult(resultJson, resultType),
                    errorJson,
                    postponedUntil,
                    epoch,
                    signOfLife
                );
            }
        }

        return null;
    }
}