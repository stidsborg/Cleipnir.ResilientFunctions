﻿using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    private readonly PostgreSqlMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;

    private readonly PostgresStatesStore _statesStore;
    public IStatesStore StatesStore => _statesStore;
    
    private readonly PostgresEffectsStore _effectsStore;
    public IEffectsStore EffectsStore => _effectsStore;

    private readonly PostgreSqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    public Utilities Utilities { get; }
    private readonly PostgresSqlUnderlyingRegister _postgresSqlUnderlyingRegister;

    public PostgreSqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tablePrefix = tablePrefix == "" ? "rfunctions" : tablePrefix;
        _connectionString = connectionString;
        
        _messageStore = new PostgreSqlMessageStore(connectionString, _tablePrefix);
        _effectsStore = new PostgresEffectsStore(connectionString, _tablePrefix);
        _statesStore = new PostgresStatesStore(connectionString, _tablePrefix);
        _timeoutStore = new PostgreSqlTimeoutStore(connectionString, _tablePrefix);
        _postgresSqlUnderlyingRegister = new(connectionString, _tablePrefix);
        Utilities = new Utilities(_postgresSqlUnderlyingRegister);
    } 

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    public async Task Initialize()
    {
        await _postgresSqlUnderlyingRegister.Initialize();
        await _messageStore.Initialize();
        await _statesStore.Initialize();
        await _effectsStore.Initialize();
        await _timeoutStore.Initialize();
        await using var conn = await CreateConnection();
        var sql = $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix} (
                function_type_id VARCHAR(200) NOT NULL,
                function_instance_id VARCHAR(200) NOT NULL,
                param_json TEXT NULL,            
                status INT NOT NULL DEFAULT {(int) Status.Executing},
                result_json TEXT NULL,
                default_state TEXT NULL,
                exception_json TEXT NULL,
                postponed_until BIGINT NULL,
                epoch INT NOT NULL DEFAULT 0,
                lease_expiration BIGINT NOT NULL,
                interrupt_count BIGINT NOT NULL DEFAULT 0,
                timestamp BIGINT NOT NULL,
                PRIMARY KEY (function_type_id, function_instance_id)
            );
            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}_executing
            ON {_tablePrefix}(function_type_id, lease_expiration, function_instance_id)
            INCLUDE (epoch)
            WHERE status = {(int) Status.Executing};

            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}_postponed
            ON {_tablePrefix}(function_type_id, postponed_until, function_instance_id)
            INCLUDE (epoch)
            WHERE status = {(int) Status.Postponed};
            ";

        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DropIfExists()
    {
        await _postgresSqlUnderlyingRegister.DropUnderlyingTable();
        await _messageStore.DropUnderlyingTable();
        await _timeoutStore.DropUnderlyingTable();
        
        await using var conn = await CreateConnection();
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task TruncateTable()
    {
        await _messageStore.TruncateTable();
        await _timeoutStore.Truncate();
        await _postgresSqlUnderlyingRegister.TruncateTable();
        await _effectsStore.Truncate();
        await _statesStore.Truncate();
        
        await using var conn = await CreateConnection();
        var sql = $"TRUNCATE TABLE {_tablePrefix}";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> CreateFunction(
        FunctionId functionId, 
        string? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        await using var conn = await CreateConnection();
        
        var sql = @$"
            INSERT INTO {_tablePrefix}
                (function_type_id, function_instance_id, status, param_json, lease_expiration, postponed_until, timestamp)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING;";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = (int) (postponeUntil == null ? Status.Executing : Status.Postponed)},
                new() {Value = param == null ? DBNull.Value : param},
                new() {Value = leaseExpiration},
                new() {Value = postponeUntil == null ? DBNull.Value : postponeUntil.Value},
                new() {Value = timestamp}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateConnection();

        var sql = @$"
            UPDATE {_tablePrefix}
            SET epoch = epoch + 1, status = {(int)Status.Executing}, lease_expiration = $1
            WHERE function_type_id = $2 AND function_instance_id = $3 AND epoch = $4
            RETURNING               
                param_json, 
                status,
                result_json, 
                default_state,
                exception_json,
                postponed_until,
                epoch, 
                lease_expiration,
                interrupt_count,
                timestamp";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = leaseExpiration },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;

        var sf = await ReadToStoredFunction(functionId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    public async Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}
            SET lease_expiration = $1
            WHERE function_type_id = $2 AND function_instance_id = $3 AND epoch = $4";
        await using var command = new NpgsqlCommand(sql, conn)
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
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT function_instance_id, epoch, lease_expiration 
            FROM {_tablePrefix}
            WHERE function_type_id = $1 AND lease_expiration < $2 AND status = {(int) Status.Executing}";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionTypeId.Value},
                new () {Value = leaseExpiresBefore }
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
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT function_instance_id, epoch, postponed_until
            FROM {_tablePrefix}
            WHERE function_type_id = $1 AND status = {(int) Status.Postponed} AND postponed_until <= $2";
        await using var command = new NpgsqlCommand(sql, conn)
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
        string? param, string? result, 
        StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch)
    {
        await using var conn = await CreateConnection();
       
        var sql = $@"
            UPDATE {_tablePrefix}
            SET status = $1,
                param_json = $2, 
                result_json = $3, 
                exception_json = $4, postponed_until = $5,
                epoch = epoch + 1
            WHERE 
                function_type_id = $6 AND 
                function_instance_id = $7 AND 
                epoch = $8";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = param == null ? DBNull.Value : param},
                new() {Value = result == null ? DBNull.Value : result},
                new() {Value = storedException == null ? DBNull.Value : JsonSerializer.Serialize(storedException)},
                new() {Value = postponeUntil ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SucceedFunction(
        FunctionId functionId, 
        string? result, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Succeeded}, result_json = $1, default_state = $2, timestamp = $3
            WHERE 
                function_type_id = $4 AND 
                function_instance_id = $5 AND 
                epoch = $6";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = result == null ? DBNull.Value : result },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> PostponeFunction(
        FunctionId functionId, 
        long postponeUntil, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Postponed}, postponed_until = $1, default_state = $2, timestamp = $3
            WHERE 
                function_type_id = $4 AND 
                function_instance_id = $5 AND 
                epoch = $6";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = postponeUntil },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}
            SET status = {(int) Status.Failed}, exception_json = $1, default_state = $2, timestamp = $3
            WHERE 
                function_type_id = $4 AND 
                function_instance_id = $5 AND 
                epoch = $6";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = JsonSerializer.Serialize(storedException) },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SuspendFunction(
        FunctionId functionId, 
        long expectedInterruptCount, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();

        var postponeSql = $@"
            UPDATE {_tablePrefix}
            SET status = {(int)Status.Suspended}, default_state = $1, timestamp = $2
            WHERE function_type_id = $3 AND 
                  function_instance_id = $4 AND 
                  epoch = $5 AND
                  interrupt_count = $6";
        await using var command = new NpgsqlCommand(postponeSql, conn)
        {
            Parameters =
            {
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
                new() { Value = expectedInterruptCount },
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task SetDefaultState(FunctionId functionId, string? stateJson)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}
            SET default_state = $1
            WHERE function_type_id = $2 AND function_instance_id = $3";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = stateJson ?? (object) DBNull.Value},
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> SetParameters(
        FunctionId functionId,
        string? param, string? result,
        int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        
        var sql = $@"
            UPDATE {_tablePrefix}
            SET param_json = $1,             
                result_json = $2, 
                epoch = epoch + 1
            WHERE function_type_id = $3 AND function_instance_id = $4 AND epoch = $5";
        
        var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = param ?? (object) DBNull.Value },
                new() { Value = result ?? (object) DBNull.Value },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };

        await using var _ = command;
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> IncrementInterruptCount(FunctionId functionId)
    {
        await using var conn = await CreateConnection();

        var postponeSql = $@"
                UPDATE {_tablePrefix}
                SET interrupt_count = interrupt_count + 1
                WHERE function_type_id = $1 AND function_instance_id = $2  AND status = {(int) Status.Executing};";
        await using var command = new NpgsqlCommand(postponeSql, conn)
        {
            Parameters =
            {
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<long?> GetInterruptCount(FunctionId functionId)
    {
        await using var conn = await CreateConnection();

        var postponeSql = $@"
                SELECT interrupt_count 
                FROM {_tablePrefix}
                WHERE function_type_id = $1 AND function_instance_id = $2";
        await using var command = new NpgsqlCommand(postponeSql, conn)
        {
            Parameters =
            {
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
            }
        };
        return (long?) await command.ExecuteScalarAsync();
    }

    public async Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            SELECT status, epoch
            FROM {_tablePrefix}
            WHERE function_type_id = $1 AND function_instance_id = $2;";
        await using var command = new NpgsqlCommand(sql, conn)
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
                (Status) reader.GetInt32(0),
                Epoch: reader.GetInt32(1)
            );
        }

        return null;
    }
    
    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            SELECT               
                param_json,             
                status,
                result_json,         
                default_state,
                exception_json,
                postponed_until,
                epoch, 
                lease_expiration,
                interrupt_count,
                timestamp
            FROM {_tablePrefix}
            WHERE function_type_id = $1 AND function_instance_id = $2;";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters = { 
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(functionId, reader);
    }

    private async Task<StoredFunction?> ReadToStoredFunction(FunctionId functionId, NpgsqlDataReader reader)
    {
        /*
           0  param_json,         
           1  status,
           2  result_json,         
           3  default_state
           4  exception_json,
           5  postponed_until,
           6  epoch, 
           7  lease_expiration,
           8 interrupt_count,
           9 timestamp
         */
        while (await reader.ReadAsync())
        {
            var hasParameter = !await reader.IsDBNullAsync(0);
            var hasResult = !await reader.IsDBNullAsync(2);
            var hasDefaultState = !await reader.IsDBNullAsync(3);
            var hasException = !await reader.IsDBNullAsync(4);
            var postponedUntil = !await reader.IsDBNullAsync(5);
            
            return new StoredFunction(
                functionId,
                hasParameter ? reader.GetString(0) : null,
                Status: (Status) reader.GetInt32(1),
                Result: hasResult ? reader.GetString(2) : null, 
                DefaultState: hasDefaultState ? reader.GetString(3) : null,
                Exception: !hasException ? null : JsonSerializer.Deserialize<StoredException>(reader.GetString(4)),
                PostponedUntil: postponedUntil ? reader.GetInt64(5) : null,
                Epoch: reader.GetInt32(6),
                LeaseExpiration: reader.GetInt64(7),
                InterruptCount: reader.GetInt64(8),
                Timestamp: reader.GetInt64(9)
            );
        }

        return null;
    }
    
    public async Task DeleteFunction(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            DELETE FROM {_tablePrefix}
            WHERE function_type_id = $1
            AND function_instance_id = $2 ";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
            }
        };
       
        await command.ExecuteNonQueryAsync();
        
        await _messageStore.Truncate(functionId);
        await _effectsStore.Remove(functionId);
        await _statesStore.Remove(functionId);
        await _timeoutStore.Remove(functionId);
    }
}