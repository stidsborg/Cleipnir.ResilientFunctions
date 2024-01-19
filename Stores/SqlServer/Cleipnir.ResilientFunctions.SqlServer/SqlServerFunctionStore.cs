using System;
using System.Collections.Generic;
using System.Data;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerFunctionStore : IFunctionStore
{
    private readonly Func<Task<SqlConnection>> _connFunc;
    private readonly string _tablePrefix;

    private readonly SqlServerTimeoutStore _timeoutStore;
    
    private readonly SqlServerActivityStore _activityStore;
    public IActivityStore ActivityStore => _activityStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;

    private readonly SqlServerMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;
    public Utilities Utilities { get; }
    private readonly SqlServerUnderlyingRegister _underlyingRegister;

    public SqlServerFunctionStore(string connectionString, string tablePrefix = "")
    {
        _connFunc = CreateConnection(connectionString);
        _tablePrefix = tablePrefix;
        _messageStore = new SqlServerMessageStore(connectionString, tablePrefix);
        _timeoutStore = new SqlServerTimeoutStore(connectionString, tablePrefix);
        _underlyingRegister = new SqlServerUnderlyingRegister(connectionString, tablePrefix);
        _activityStore = new SqlServerActivityStore(connectionString, tablePrefix);
        Utilities = new Utilities(_underlyingRegister);
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
        await _underlyingRegister.Initialize();
        await _messageStore.Initialize();
        await _activityStore.Initialize();
        await _timeoutStore.Initialize();
        await using var conn = await _connFunc();
        var sql = @$"    
            CREATE TABLE {_tablePrefix}RFunctions (
                FunctionTypeId NVARCHAR(200) NOT NULL,
                FunctionInstanceId NVARCHAR(200) NOT NULL,
                ParamJson NVARCHAR(MAX) NOT NULL,
                ParamType NVARCHAR(255) NOT NULL,
                ScrapbookJson NVARCHAR(MAX) NOT NULL,
                ScrapbookType NVARCHAR(255) NOT NULL,
                Status INT NOT NULL,
                ResultJson NVARCHAR(MAX) NULL,
                ResultType NVARCHAR(255) NULL,
                ExceptionJson NVARCHAR(MAX) NULL,
                PostponedUntil BIGINT NULL,            
                Epoch INT NOT NULL,
                LeaseExpiration BIGINT NOT NULL,
                Timestamp BIGINT NOT NULL,
                PRIMARY KEY (FunctionTypeId, FunctionInstanceId)
            );
            CREATE INDEX {_tablePrefix}RFunctions_idx_Executing
                ON {_tablePrefix}RFunctions (FunctionTypeId, LeaseExpiration, FunctionInstanceId)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Executing};
            CREATE INDEX {_tablePrefix}RFunctions_idx_Postponed
                ON {_tablePrefix}RFunctions (FunctionTypeId, PostponedUntil, FunctionInstanceId)
                INCLUDE (Epoch)
                WHERE Status = {(int)Status.Postponed};";

        await using var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }
    
    public async Task DropIfExists()
    {
        await _underlyingRegister.DropUnderlyingTable();
        await _messageStore.DropUnderlyingTable();
        await _timeoutStore.DropUnderlyingTable();
        
        await using var conn = await _connFunc();
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}RFunctions";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Truncate()
    {
        await _underlyingRegister.TruncateTable();
        await _messageStore.TruncateTable();
        await _timeoutStore.TruncateTable();
        
        await using var conn = await _connFunc();
        var sql = $"TRUNCATE TABLE {_tablePrefix}RFunctions";
        await using var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        StoredScrapbook storedScrapbook, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        await using var conn = await _connFunc();
        
        try
        {
            var sql = @$"
                INSERT INTO {_tablePrefix}RFunctions(
                    FunctionTypeId, FunctionInstanceId, 
                    ParamJson, ParamType, 
                    ScrapbookJson, ScrapbookType, 
                    Status,
                    Epoch, 
                    LeaseExpiration,
                    PostponedUntil,
                    Timestamp)
                VALUES(
                    @FunctionTypeId, @FunctionInstanceId, 
                    @ParamJson, @ParamType,  
                    @ScrapbookJson, @ScrapbookType,
                    {(int) (postponeUntil == null ? Status.Executing : Status.Postponed)},
                    0, 
                    @LeaseExpiration,
                    @PostponeUntil,
                    @Timestamp
                )";

            await using var command = new SqlCommand(sql, conn);
            
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@ParamJson", param.ParamJson);
            command.Parameters.AddWithValue("@ParamType", param.ParamType);
            command.Parameters.AddWithValue("@ScrapbookJson", storedScrapbook.ScrapbookJson);
            command.Parameters.AddWithValue("@ScrapbookType", storedScrapbook.ScrapbookType);
            command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiration);
            command.Parameters.AddWithValue("@PostponeUntil", postponeUntil == null ? DBNull.Value : postponeUntil.Value);
            command.Parameters.AddWithValue("@Timestamp", timestamp);

            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException sqlException) when (sqlException.Number == SqlError.UNIQUENESS_VIOLATION)
        {
            return false;
        }

        return true;
    }

    public async Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Epoch = Epoch + 1, 
                Status = {(int)Status.Executing}, 
                LeaseExpiration = @LeaseExpiration
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch;

            SELECT ParamJson, ParamType,
                   ScrapbookJson, ScrapbookType,
                   Status,
                   ResultJson, ResultType,
                   ExceptionJson,
                   PostponedUntil,
                   Epoch, 
                   LeaseExpiration,
                   Timestamp
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiration);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        await using var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;
        
        var sf = ReadToStoredFunction(functionId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    public async Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET LeaseExpiration = @LeaseExpiration
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @Epoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiration);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@Epoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT FunctionInstanceId, Epoch, LeaseExpiration
            FROM {_tablePrefix}RFunctions WITH (NOLOCK)
            WHERE FunctionTypeId = @FunctionTypeId AND LeaseExpiration < @LeaseExpiration AND Status = {(int) Status.Executing}";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionTypeId.Value);
        command.Parameters.AddWithValue("@LeaseExpiration", leaseExpiresBefore);

        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<StoredExecutingFunction>(); 
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var functionInstanceId = reader.GetString(0);
                var epoch = reader.GetInt32(1);
                var expiration = reader.GetInt64(2);
                rows.Add(new StoredExecutingFunction(functionInstanceId, epoch, expiration));    
            }

            reader.NextResult();
        }

        return rows;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT FunctionInstanceId, Epoch, PostponedUntil
            FROM {_tablePrefix}RFunctions 
            WHERE FunctionTypeId = @FunctionTypeId 
              AND Status = {(int) Status.Postponed} 
              AND PostponedUntil <= @PostponedUntil";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionTypeId.Value);
        command.Parameters.AddWithValue("@PostponedUntil", isEligibleBefore);

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
        FunctionId functionId, Status status, 
        StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
    
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET
                Status = @Status,
                ParamJson = @ParamJson, ParamType = @ParamType,            
                ScrapbookJson = @ScrapbookJson, ScrapbookType = @ScrapbookType,
                ResultJson = @ResultJson, ResultType = @ResultType,
                ExceptionJson = @ExceptionJson,
                PostponedUntil = @PostponedUntil,
                Epoch = Epoch + 1
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@Status", (int) status);
        command.Parameters.AddWithValue("@ParamJson", storedParameter.ParamJson);
        command.Parameters.AddWithValue("@ParamType", storedParameter.ParamType);
        command.Parameters.AddWithValue("@ScrapbookJson", storedScrapbook.ScrapbookJson);
        command.Parameters.AddWithValue("@ScrapbookType", storedScrapbook.ScrapbookType);
        command.Parameters.AddWithValue("@ResultJson", storedResult?.ResultJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ResultType", storedResult?.ResultType ?? (object) DBNull.Value);
        var exceptionJson = storedException == null ? null : JsonSerializer.Serialize(storedException);
        command.Parameters.AddWithValue("@ExceptionJson", exceptionJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@PostponedUntil", postponeUntil ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SaveScrapbookForExecutingFunction( 
        FunctionId functionId,
        string scrapbookJson,
        int expectedEpoch,
        ComplimentaryState.SaveScrapbookForExecutingFunction _)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET ScrapbookJson = @ScrapbookJson
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SetParameters(
        FunctionId functionId,
        StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET ParamJson = @ParamJson, ParamType = @ParamType, 
                ScrapbookJson = @ScrapbookJson, ScrapbookType = @ScrapbookType, 
                ResultJson = @ResultJson, ResultType = @ResultType,
                Epoch = Epoch + 1
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ParamJson", storedParameter.ParamJson);
        command.Parameters.AddWithValue("@ParamType", storedParameter.ParamType);
        command.Parameters.AddWithValue("@ScrapbookJson", storedScrapbook.ScrapbookJson);
        command.Parameters.AddWithValue("@ScrapbookType", storedScrapbook.ScrapbookType);
        command.Parameters.AddWithValue("@ResultJson", storedResult.ResultJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ResultType", storedResult.ResultType ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SucceedFunction(
        FunctionId functionId, 
        StoredResult result, 
        string scrapbookJson,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState.SetResult _)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Succeeded}, ResultJson = @ResultJson, ResultType = @ResultType, ScrapbookJson = @ScrapbookJson, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ResultJson", result.ResultJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ResultType", result.ResultType ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> PostponeFunction(
        FunctionId functionId, 
        long postponeUntil, 
        string scrapbookJson,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState.SetResult _)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Postponed}, PostponedUntil = @PostponedUntil, ScrapbookJson = @ScrapbookJson, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@PostponedUntil", postponeUntil);
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SuspendFunction(
        FunctionId functionId, 
        int expectedMessageCount, 
        string scrapbookJson,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState.SetResult _)
    {
        {
            await using var conn = await _connFunc();
            await using var transaction = (SqlTransaction) await conn.BeginTransactionAsync(IsolationLevel.Serializable);
            
            var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Suspended}, ScrapbookJson = @ScrapbookJson, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}RFunctions_Messages WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId) = @ExpectedCount
            AND FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

            await using var command = new SqlCommand(sql, conn, transaction);
            command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
            command.Parameters.AddWithValue("@Timestamp", timestamp);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
            command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);
            command.Parameters.AddWithValue("@ExpectedCount", expectedMessageCount);

            var affectedRows = await command.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
            if (affectedRows > 0)
                return true;
        }
        {
            await using var conn = await _connFunc();
            var sql = @$"
                SELECT COALESCE(MAX(position), -1) + 1 
                FROM {_tablePrefix}RFunctions_Messages 
                WHERE FunctionTypeId = @FunctionTypeId 
                  AND FunctionInstanceId = @FunctionInstanceId";

            await using var command = new SqlCommand(sql, conn);
            command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
            command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);

            var numberOfMessages = (int?) await command.ExecuteScalarAsync();
            if (numberOfMessages == expectedMessageCount) 
                return false;

            return await PostponeFunction(
                functionId,
                postponeUntil: 0, scrapbookJson, timestamp, expectedEpoch,
                new ComplimentaryState.SetResult()
            );
        }
    }

    public async Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            SELECT Status, Epoch
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        while (reader.HasRows)
            while (reader.Read())
            {
                var status = (Status) reader.GetInt32(0);
                var epoch = reader.GetInt32(1);

                return new StatusAndEpoch(status, epoch);
            }

        return null;
    }

    public async Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException, 
        string scrapbookJson,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState.SetResult _)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Failed}, ExceptionJson = @ExceptionJson, ScrapbookJson = @ScrapbookJson, Timestamp = @timestamp, Epoch = @ExpectedEpoch
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ExceptionJson", JsonSerializer.Serialize(storedException));
        command.Parameters.AddWithValue("@ScrapbookJson", scrapbookJson);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
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
                    ExceptionJson,
                    PostponedUntil,
                    Epoch, 
                    LeaseExpiration,
                    Timestamp
            FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        
        await using var reader = await command.ExecuteReaderAsync();
        return ReadToStoredFunction(functionId, reader);
    }

    private StoredFunction? ReadToStoredFunction(FunctionId functionId, SqlDataReader reader)
    {
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var paramJson = reader.GetString(0);
                var paramType = reader.GetString(1);
                var scrapbookJson = reader.GetString(2);
                var scrapbookType = reader.GetString(3);
                var status = (Status) reader.GetInt32(4);
                var resultJson = reader.IsDBNull(5) ? null : reader.GetString(5);
                var resultType = reader.IsDBNull(6) ? null : reader.GetString(6);
                var exceptionJson = reader.IsDBNull(7) ? null : reader.GetString(7);
                var storedException = exceptionJson == null
                    ? null
                    : JsonSerializer.Deserialize<StoredException>(exceptionJson);
                var postponedUntil = reader.IsDBNull(8) ? default(long?) : reader.GetInt64(8);
                var epoch = reader.GetInt32(9);
                var leaseExpiration = reader.GetInt64(10);
                var timestamp = reader.GetInt64(11);

                return new StoredFunction(
                    functionId,
                    new StoredParameter(paramJson, paramType),
                    new StoredScrapbook(scrapbookJson, scrapbookType),
                    status,
                    new StoredResult(resultJson, resultType),
                    storedException,
                    postponedUntil,
                    epoch,
                    leaseExpiration,
                    timestamp
                );
            }
        }

        return default;
    }

    public async Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            DELETE FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId ";

        if (expectedEpoch != null)
            sql += "AND Epoch = @ExpectedEpoch ";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        if (expectedEpoch != null)
            command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch.Value);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
}