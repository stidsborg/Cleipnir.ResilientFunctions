﻿using System.Data;
using System.Text.Json;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;
using static Cleipnir.ResilientFunctions.MySQL.DatabaseHelper;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    private readonly MySqlMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;
    private readonly MySqlActivityStore _activityStore;
    public IActivityStore ActivityStore => _activityStore;
    private readonly MySqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    public Utilities Utilities { get; }
    private readonly MySqlUnderlyingRegister _mySqlUnderlyingRegister;

    public MySqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _messageStore = new MySqlMessageStore(connectionString, tablePrefix);
        _activityStore = new MySqlActivityStore(connectionString, tablePrefix);
        _timeoutStore = new MySqlTimeoutStore(connectionString, tablePrefix);
        _mySqlUnderlyingRegister = new(connectionString, _tablePrefix);
        Utilities = new Utilities(_mySqlUnderlyingRegister);
    }

    public async Task Initialize()
    {
        await _mySqlUnderlyingRegister.Initialize();
        await MessageStore.Initialize();
        await ActivityStore.Initialize();
        await TimeoutStore.Initialize();
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions (
                function_type_id VARCHAR(200) NOT NULL,
                function_instance_id VARCHAR(200) NOT NULL,
                param_json TEXT NOT NULL,
                param_type VARCHAR(255) NOT NULL,
                state_json TEXT NOT NULL,
                state_type VARCHAR(255) NOT NULL,
                status INT NOT NULL,
                result_json TEXT NULL,
                result_type VARCHAR(255) NULL,
                exception_json TEXT NULL,
                postponed_until BIGINT NULL,
                epoch INT NOT NULL,
                lease_expiration BIGINT NOT NULL,
                signal_count BIGINT NOT NULL DEFAULT 0,
                timestamp BIGINT NOT NULL,
                PRIMARY KEY (function_type_id, function_instance_id),
                INDEX (function_type_id, status, function_instance_id)   
            );";

        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropIfExists()
    {
        await _messageStore.DropUnderlyingTable();
        await _mySqlUnderlyingRegister.DropUnderlyingTable();
        await _timeoutStore.DropUnderlyingTable();

        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task TruncateTables()
    {
        await _messageStore.TruncateTable();
        await _timeoutStore.TruncateTable();
        await _mySqlUnderlyingRegister.TruncateTable();
        
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $"TRUNCATE TABLE {_tablePrefix}rfunctions";
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        StoredState storedState, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        await using var conn = await CreateOpenConnection(_connectionString);

        var status = postponeUntil == null ? Status.Executing : Status.Postponed;
        var sql = @$"
            INSERT IGNORE INTO {_tablePrefix}rfunctions
                (function_type_id, function_instance_id, param_json, param_type, state_json, state_type, status, epoch, lease_expiration, postponed_until, timestamp)
            VALUES
                (?, ?, ?, ?, ?, ?, {(int) status}, 0, ?, ?, ?)";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = param.ParamJson},
                new() {Value = param.ParamType},
                new() {Value = storedState.StateJson},
                new() {Value = storedState.StateType},
                new() {Value = leaseExpiration},
                new() {Value = postponeUntil},
                new() {Value = timestamp}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            UPDATE {_tablePrefix}rfunctions
            SET epoch = epoch + 1, status = {(int)Status.Executing}, lease_expiration = ?
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ?;
            SELECT               
                param_json, 
                param_type,
                state_json, 
                state_type,
                status,
                result_json, 
                result_type,
                exception_json,
                postponed_until,
                epoch, 
                lease_expiration,
                signal_count,
                timestamp
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = leaseExpiration },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
            }
        };

        var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;

        var sf = await ReadToStoredFunction(functionId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    public async Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET lease_expiration = ?
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ? AND status = {(int) Status.Executing}";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = leaseExpiration},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            SELECT function_instance_id, epoch, lease_expiration 
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND lease_expiration < ? AND status = {(int) Status.Executing}";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId.Value},
                new() { Value = leaseExpiresBefore }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredExecutingFunction>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            var expiration = reader.GetInt64(2);
            functions.Add(new StoredExecutingFunction(functionInstanceId, epoch, expiration));
        }
        
        return functions;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = @$"
            SELECT function_instance_id, epoch, postponed_until
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND status = {(int) Status.Postponed} AND postponed_until <= ?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId.Value},
                new() {Value = isEligibleBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<StoredPostponedFunction>();
        while (await reader.ReadAsync())
        {
            var functionInstanceId = reader.GetString(0);
            var epoch = reader.GetInt32(1);
            var postponedUntil = reader.GetInt64(2);
            functions.Add(new StoredPostponedFunction(functionInstanceId, epoch, postponedUntil));
        }
        
        return functions;
    }

    public async Task<bool> SetFunctionState(
        FunctionId functionId, Status status, 
        StoredParameter storedParameter, StoredState storedState, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
      
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = ?, 
                param_json = ?, param_type = ?, 
                state_json = ?, state_type = ?, 
                result_json = ?, result_type = ?, 
                exception_json = ?, postponed_until = ?,
                epoch = epoch + 1
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = storedParameter.ParamJson},
                new() {Value = storedParameter.ParamType},
                new() {Value = storedState.StateJson},
                new() {Value = storedState.StateType},
                new() {Value = storedResult.ResultJson ?? (object) DBNull.Value},
                new() {Value = storedResult.ResultType ?? (object) DBNull.Value},
                new() {Value = storedException != null ? JsonSerializer.Serialize(storedException) : DBNull.Value},
                new() {Value = postponeUntil ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SaveStateForExecutingFunction( 
        FunctionId functionId,
        string stateJson,
        int expectedEpoch,
        ComplimentaryState _)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET state_json = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = stateJson},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SetParameters(
        FunctionId functionId,
        StoredParameter storedParameter, StoredState storedState, StoredResult storedResult,
        int expectedEpoch)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
      
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET param_json = ?, param_type = ?, 
                state_json = ?, state_type = ?, 
                result_json = ?, result_type = ?,
                epoch = epoch + 1
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = storedParameter.ParamJson },
                new() { Value = storedParameter.ParamType },
                new() { Value = storedState.StateJson },
                new() { Value = storedState.StateType },
                new() { Value = storedResult.ResultJson },
                new() { Value = storedResult.ResultType },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
            
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Succeeded}, result_json = ?, result_type = ?, state_json = ?, timestamp = ?, epoch = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = result?.ResultJson ?? (object)DBNull.Value },
                new() { Value = result?.ResultType ?? (object)DBNull.Value },
                new() { Value = stateJson },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Postponed}, postponed_until = ?, state_json = ?, timestamp = ?, epoch = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = postponeUntil },
                new() { Value = stateJson },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> SuspendFunction(FunctionId functionId, int expectedMessageCount, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Postponed}, postponed_until = ?, state_json = ?, timestamp = ?
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ?;

            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Suspended}
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ? AND 
                (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}rfunctions_messages WHERE function_type_id = ? AND function_instance_id = ?) = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = DateTime.UtcNow.AddSeconds(30).Ticks },
                new() { Value = stateJson },
                new() { Value = timestamp },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
                
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
                
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedMessageCount },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        if (affectedRows == 0)
            return false;
        if (affectedRows == 2)
            return true;
        
        // affectedRows == 1
        sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Postponed}, postponed_until = 0
            WHERE function_type_id = ? AND function_instance_id = ? AND epoch = ?;";

        await using var postponeCommand = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        await postponeCommand.ExecuteNonQueryAsync();
        return true;
    }

    public async Task IncrementSignalCount(FunctionId functionId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET signal_count = signal_count + 1
            WHERE function_type_id = ? AND function_instance_id = ?;";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
            }
        };
        
        await command.ExecuteNonQueryAsync();
    }

    public async Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            SELECT status, epoch
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters = { 
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            return new StatusAndEpoch(
                Status: (Status) reader.GetInt32(0),
                Epoch: reader.GetInt32(1)
            );
        }

        return null;
    }

    public async Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Failed}, exception_json = ?, state_json = ?, timestamp = ?, epoch = ?
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ? AND 
                epoch = ?";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = JsonSerializer.Serialize(storedException) },
                new() { Value = stateJson },
                new() { Value = timestamp },
                new() { Value = expectedEpoch },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        var sql = $@"
            SELECT               
                param_json, 
                param_type,
                state_json, 
                state_type,
                status,
                result_json, 
                result_type,
                exception_json,
                postponed_until,
                epoch, 
                lease_expiration,
                signal_count,
                timestamp
            FROM {_tablePrefix}rfunctions
            WHERE function_type_id = ? AND function_instance_id = ?;";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters = { 
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(functionId, reader);
    }

    private async Task<StoredFunction?> ReadToStoredFunction(FunctionId functionId, MySqlDataReader reader)
    {
        while (await reader.ReadAsync())
        {
            var hasResult = !await reader.IsDBNullAsync(6);
            var hasError = !await reader.IsDBNullAsync(7);
            var storedException = hasError
                ? JsonSerializer.Deserialize<StoredException>(reader.GetString(7))
                : null;
            var postponedUntil = !await reader.IsDBNullAsync(8);
            return new StoredFunction(
                functionId,
                new StoredParameter(reader.GetString(0), reader.GetString(1)),
                State: new StoredState(reader.GetString(2), reader.GetString(3)),
                Status: (Status) reader.GetInt32(4),
                Result: new StoredResult(
                    hasResult ? reader.GetString(5) : null, 
                    hasResult ? reader.GetString(6) : null
                ),
                storedException,
                postponedUntil ? reader.GetInt64(8) : null,
                Epoch: reader.GetInt32(9),
                LeaseExpiration: reader.GetInt64(10),
                SignalCount: reader.GetInt64(11),
                Timestamp: reader.GetInt64(12)
            );
        }

        return null;
    }

    public async Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $@"
            START TRANSACTION;
            DELETE FROM {_tablePrefix}rfunctions_messages
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ?;
            DELETE FROM {_tablePrefix}rfunctions
            WHERE 
                function_type_id = ? AND 
                function_instance_id = ?";
        
        if (expectedEpoch != null)
            sql += " AND epoch = ? ";

        sql += "; COMMIT";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        if (expectedEpoch != null)
            command.Parameters.Add(new() { Value = expectedEpoch.Value });

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows > 0;
    }
}