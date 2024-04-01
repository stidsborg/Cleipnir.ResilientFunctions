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
    private readonly SqlServerEffectsStore _effectsStore;
    private readonly SqlServerStatesStore _statesStore;
    private readonly SqlServerMessageStore _messageStore;
    
    public IEffectsStore EffectsStore => _effectsStore;
    public IStatesStore StatesStore => _statesStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
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
        _effectsStore = new SqlServerEffectsStore(connectionString, tablePrefix);
        _statesStore = new SqlServerStatesStore(connectionString, tablePrefix);
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
        await _effectsStore.Initialize();
        await _statesStore.Initialize();
        await _timeoutStore.Initialize();
        await using var conn = await _connFunc();
        var sql = @$"    
            CREATE TABLE {_tablePrefix}RFunctions (
                FunctionTypeId NVARCHAR(200) NOT NULL,
                FunctionInstanceId NVARCHAR(200) NOT NULL,
                ParamJson NVARCHAR(MAX) NOT NULL,
                ParamType NVARCHAR(255) NOT NULL,            
                Status INT NOT NULL,
                ResultJson NVARCHAR(MAX) NULL,
                ResultType NVARCHAR(255) NULL,
                ExceptionJson NVARCHAR(MAX) NULL,
                PostponedUntil BIGINT NULL,            
                Epoch INT NOT NULL,
                LeaseExpiration BIGINT NOT NULL,
                InterruptCount BIGINT NOT NULL DEFAULT 0,
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
                    Status,
                    Epoch, 
                    LeaseExpiration,
                    PostponedUntil,
                    Timestamp)
                VALUES(
                    @FunctionTypeId, @FunctionInstanceId, 
                    @ParamJson, @ParamType,  
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
                   Status,
                   ResultJson, ResultType,
                   ExceptionJson,
                   PostponedUntil,
                   Epoch, 
                   LeaseExpiration,
                   InterruptCount,
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
        StoredParameter storedParameter, StoredResult storedResult, 
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

    public async Task<bool> SaveStateForExecutingFunction( 
        FunctionId functionId,
        string stateJson,
        int expectedEpoch,
        ComplimentaryState _)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET stateJson = @StateJson
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@StateJson", stateJson);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SetParameters(
        FunctionId functionId,
        StoredParameter storedParameter, StoredResult storedResult,
        int expectedEpoch)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET ParamJson = @ParamJson, ParamType = @ParamType, 
                ResultJson = @ResultJson, ResultType = @ResultType,
                Epoch = Epoch + 1
            WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ParamJson", storedParameter.ParamJson);
        command.Parameters.AddWithValue("@ParamType", storedParameter.ParamType);
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
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState _)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Succeeded}, ResultJson = @ResultJson, ResultType = @ResultType, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ResultJson", result.ResultJson ?? (object) DBNull.Value);
        command.Parameters.AddWithValue("@ResultType", result.ResultType ?? (object) DBNull.Value);
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
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState _)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Postponed}, PostponedUntil = @PostponedUntil, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@PostponedUntil", postponeUntil);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }

    public async Task<bool> SuspendFunction(
        FunctionId functionId,
        long expectedInterruptCount,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState _)
    {
        await using var conn = await _connFunc();

        var sql = @$"
                UPDATE {_tablePrefix}RFunctions
                SET Status = {(int)Status.Suspended}, Timestamp = @Timestamp
                WHERE FunctionTypeId = @FunctionTypeId AND 
                      FunctionInstanceId = @FunctionInstanceId AND 
                      Epoch = @ExpectedEpoch AND
                      InterruptCount = @ExpectedInterruptCount;";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@Timestamp", timestamp);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@ExpectedEpoch", expectedEpoch);
        command.Parameters.AddWithValue("@ExpectedInterruptCount", expectedInterruptCount);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> IncrementInterruptCount(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
                UPDATE {_tablePrefix}RFunctions
                SET InterruptCount = InterruptCount + 1
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId AND Status = {(int) Status.Executing};";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<long?> GetInterruptCount(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
                SELECT InterruptCount 
                FROM {_tablePrefix}RFunctions            
                WHERE FunctionTypeId = @FunctionTypeId AND FunctionInstanceId = @FunctionInstanceId;";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);

        var interruptCount = await command.ExecuteScalarAsync();
        return (long?) interruptCount;
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
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState _)
    {
        await using var conn = await _connFunc();
        
        var sql = @$"
            UPDATE {_tablePrefix}RFunctions
            SET Status = {(int) Status.Failed}, ExceptionJson = @ExceptionJson, Timestamp = @timestamp, Epoch = @ExpectedEpoch
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId
            AND Epoch = @ExpectedEpoch";

        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@ExceptionJson", JsonSerializer.Serialize(storedException));
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
                    Status,
                    ResultJson, ResultType,
                    ExceptionJson,
                    PostponedUntil,
                    Epoch, 
                    LeaseExpiration,
                    InterruptCount,
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
                var status = (Status) reader.GetInt32(2);
                var resultJson = reader.IsDBNull(3) ? null : reader.GetString(3);
                var resultType = reader.IsDBNull(4) ? null : reader.GetString(4);
                var exceptionJson = reader.IsDBNull(5) ? null : reader.GetString(5);
                var storedException = exceptionJson == null
                    ? null
                    : JsonSerializer.Deserialize<StoredException>(exceptionJson);
                var postponedUntil = reader.IsDBNull(6) ? default(long?) : reader.GetInt64(6);
                var epoch = reader.GetInt32(7);
                var leaseExpiration = reader.GetInt64(8);
                var interruptCount = reader.GetInt64(9);
                var timestamp = reader.GetInt64(10);

                return new StoredFunction(
                    functionId,
                    new StoredParameter(paramJson, paramType),
                    status,
                    new StoredResult(resultJson, resultType),
                    storedException,
                    postponedUntil,
                    epoch,
                    leaseExpiration,
                    timestamp,
                    interruptCount
                );
            }
        }

        return default;
    }

    public async Task DeleteFunction(FunctionId functionId)
    {
        await using var conn = await _connFunc();
        var sql = @$"
            DELETE FROM {_tablePrefix}RFunctions
            WHERE FunctionTypeId = @FunctionTypeId
            AND FunctionInstanceId = @FunctionInstanceId ";
        
        await using var command = new SqlCommand(sql, conn);
        command.Parameters.AddWithValue("@FunctionInstanceId", functionId.InstanceId.Value);
        command.Parameters.AddWithValue("@FunctionTypeId", functionId.TypeId.Value);
        
        await command.ExecuteNonQueryAsync();
        
        await _messageStore.Truncate(functionId);
        await _effectsStore.Remove(functionId);
        await _statesStore.Remove(functionId);
        await _timeoutStore.Remove(functionId);
    }
}