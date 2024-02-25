using System;
using System.Collections.Generic;
using System.Data;
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
    private readonly PostgresActivityStore _activityStore;
    public IActivityStore ActivityStore => _activityStore;

    private readonly PostgreSqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    public Utilities Utilities { get; }
    private readonly PostgresSqlUnderlyingRegister _postgresSqlUnderlyingRegister;

    public PostgreSqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _messageStore = new PostgreSqlMessageStore(connectionString, tablePrefix);
        _activityStore = new PostgresActivityStore(connectionString, tablePrefix);
        _timeoutStore = new PostgreSqlTimeoutStore(connectionString, tablePrefix);
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
        await _activityStore.Initialize();
        await _timeoutStore.Initialize();
        await using var conn = await CreateConnection();
        var sql = $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunctions (
                function_type_id VARCHAR(200) NOT NULL,
                function_instance_id VARCHAR(200) NOT NULL,
                param_json TEXT NOT NULL,
                param_type VARCHAR(255) NOT NULL,
                state_json TEXT NOT NULL,
                state_type VARCHAR(255) NOT NULL,
                status INT NOT NULL DEFAULT {(int) Status.Executing},
                result_json TEXT NULL,
                result_type VARCHAR(255) NULL,
                exception_json TEXT NULL,
                postponed_until BIGINT NULL,
                epoch INT NOT NULL DEFAULT 0,
                lease_expiration BIGINT NOT NULL,
                signal_count BIGINT NOT NULL DEFAULT 0,
                timestamp BIGINT NOT NULL,
                PRIMARY KEY (function_type_id, function_instance_id)
            );
            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}rfunctions_executing
            ON {_tablePrefix}rfunctions(function_type_id, lease_expiration, function_instance_id)
            INCLUDE (epoch)
            WHERE status = {(int) Status.Executing};

            CREATE INDEX IF NOT EXISTS idx_{_tablePrefix}rfunctions_postponed
            ON {_tablePrefix}rfunctions(function_type_id, postponed_until, function_instance_id)
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
        var sql = $"DROP TABLE IF EXISTS {_tablePrefix}rfunctions";
        await using var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task TruncateTable()
    {
        await _messageStore.TruncateTable();
        await _timeoutStore.TruncateTable();
        await _postgresSqlUnderlyingRegister.TruncateTable();
        
        await using var conn = await CreateConnection();
        var sql = $"TRUNCATE TABLE {_tablePrefix}rfunctions";
        await using var command = new NpgsqlCommand(sql, conn);
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
        await using var conn = await CreateConnection();
        
        var sql = @$"
            INSERT INTO {_tablePrefix}rfunctions
                (function_type_id, function_instance_id, status, param_json, param_type, state_json, state_type, lease_expiration, postponed_until, timestamp)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT DO NOTHING;";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
                new() {Value = (int) (postponeUntil == null ? Status.Executing : Status.Postponed)},
                new() {Value = param.ParamJson},
                new() {Value = param.ParamType},
                new() {Value = storedState.StateJson},
                new() {Value = storedState.StateType},
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
            UPDATE {_tablePrefix}rfunctions
            SET epoch = epoch + 1, status = {(int)Status.Executing}, lease_expiration = $1
            WHERE function_type_id = $2 AND function_instance_id = $3 AND epoch = $4
            RETURNING               
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
            UPDATE {_tablePrefix}rfunctions
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
            FROM {_tablePrefix}rfunctions
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
            FROM {_tablePrefix}rfunctions
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
        StoredParameter storedParameter, StoredState storedState, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch)
    {
        await using var conn = await CreateConnection();
       
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = $1,
                param_json = $2, param_type = $3,
                state_json = $4, state_type = $5, 
                result_json = $6, result_type = $7, 
                exception_json = $8, postponed_until = $9,
                epoch = epoch + 1
            WHERE 
                function_type_id = $10 AND 
                function_instance_id = $11 AND 
                epoch = $12";
        await using var command = new NpgsqlCommand(sql, conn)
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

    public async Task<bool> SaveStateForExecutingFunction( 
        FunctionId functionId,
        string stateJson,
        int expectedEpoch,
        ComplimentaryState _)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET state_json = $1
            WHERE 
                function_type_id = $2 AND 
                function_instance_id = $3 AND 
                epoch = $4";
        await using var command = new NpgsqlCommand(sql, conn)
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
        await using var conn = await CreateConnection();
        
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET param_json = $1, param_type = $2, 
                state_json = $3, state_type = $4, 
                result_json = $5, result_type = $6,
                epoch = epoch + 1
            WHERE function_type_id = $7 AND function_instance_id = $8 AND epoch = $9";
        
        var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = storedParameter.ParamJson },
                new() { Value = storedParameter.ParamType },
                new() { Value = storedState.StateJson },
                new() { Value = storedState.StateType },
                new() { Value = storedResult.ResultJson ?? (object) DBNull.Value },
                new() { Value = storedResult.ResultType ?? (object) DBNull.Value },
                new() { Value = functionId.TypeId.Value },
                new() { Value = functionId.InstanceId.Value },
                new() { Value = expectedEpoch },
            }
        };

        await using var _ = command;
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    public async Task<bool> SucceedFunction(
        FunctionId functionId, 
        StoredResult result, 
        string stateJson,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Succeeded}, result_json = $1, result_type = $2, state_json = $3, timestamp = $4
            WHERE 
                function_type_id = $5 AND 
                function_instance_id = $6 AND 
                epoch = $7";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = result?.ResultJson ?? (object) DBNull.Value},
                new() {Value = result?.ResultType ?? (object) DBNull.Value},
                new() { Value = stateJson },
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
        string stateJson,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Postponed}, postponed_until = $1, state_json = $2, timestamp = $3
            WHERE 
                function_type_id = $4 AND 
                function_instance_id = $5 AND 
                epoch = $6";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = postponeUntil },
                new() { Value = stateJson },
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
        int expectedMessageCount, 
        string stateJson,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        {
            var postponeSql = $@"
                UPDATE {_tablePrefix}rfunctions
                SET status = {(int)Status.Postponed}, postponed_until = $1, state_json = $2, timestamp = $3
                WHERE function_type_id = $4 AND function_instance_id = $5 AND epoch = $6";
            await using var command = new NpgsqlCommand(postponeSql, conn)
            {
                Parameters =
                {
                    new() { Value = DateTime.UtcNow.AddSeconds(30).Ticks },
                    new() { Value = stateJson },
                    new() { Value = timestamp },
                    new() { Value = functionId.TypeId.Value },
                    new() { Value = functionId.InstanceId.Value },
                    new() { Value = expectedEpoch },
                    
                }  
            };
            var affectedRows = await command.ExecuteNonQueryAsync();
            if (affectedRows == 0)
                return false;            
        }
        {
            var suspendSql = $@"
                UPDATE {_tablePrefix}rfunctions
                SET status = {(int)Status.Suspended}
                WHERE             
                    function_type_id = $1 AND 
                    function_instance_id = $2 AND 
                    epoch = $3 AND
                    (SELECT COALESCE(MAX(position), -1) + 1 FROM {_tablePrefix}rfunctions_messages WHERE function_type_id = $4 AND function_instance_id = $5) = $6";

            await using var command = new NpgsqlCommand(suspendSql, conn)
            {
                Parameters =
                {
                    new() { Value = functionId.TypeId.Value },
                    new() { Value = functionId.InstanceId.Value },
                    new() { Value = expectedEpoch },
                    new() { Value = functionId.TypeId.Value },
                    new() { Value = functionId.InstanceId.Value },
                    new() { Value = expectedMessageCount },
                }
            };
        
            var affectedRow = await command.ExecuteNonQueryAsync();
            if (affectedRow == 1)
                return true;
        }
        
        //otherwise postpone immediatly
        {
            var postponeSql = $@"
                UPDATE {_tablePrefix}rfunctions
                SET status = {(int)Status.Postponed}, postponed_until = 0
                WHERE function_type_id = $1 AND function_instance_id = $2 AND epoch = $3";
            await using var command = new NpgsqlCommand(postponeSql, conn)
            {
                Parameters =
                {
                    new() { Value = functionId.TypeId.Value },
                    new() { Value = functionId.InstanceId.Value },
                    new() { Value = expectedEpoch },
                    
                }  
            };
            await command.ExecuteNonQueryAsync();
            return true;
        }
    }

    public async Task IncrementSignalCount(FunctionId functionId)
    {
        await using var conn = await CreateConnection();

        var postponeSql = $@"
                UPDATE {_tablePrefix}rfunctions
                SET signal_count = signal_count + 1
                WHERE function_type_id = $1 AND function_instance_id = $2";
        await using var command = new NpgsqlCommand(postponeSql, conn)
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
        await using var conn = await CreateConnection();
        var sql = $@"
            SELECT status, epoch
            FROM {_tablePrefix}rfunctions
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

    public async Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        var sql = $@"
            UPDATE {_tablePrefix}rfunctions
            SET status = {(int) Status.Failed}, exception_json = $1, state_json = $2, timestamp = $3
            WHERE 
                function_type_id = $4 AND 
                function_instance_id = $5 AND 
                epoch = $6";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = JsonSerializer.Serialize(storedException) },
                new() { Value = stateJson },
                new() { Value = timestamp },
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
        await using var conn = await CreateConnection();
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
        while (await reader.ReadAsync())
        {
            var hasResult = !await reader.IsDBNullAsync(6);
            var hasException = !await reader.IsDBNullAsync(7);
            var postponedUntil = !await reader.IsDBNullAsync(8);
            
            return new StoredFunction(
                functionId,
                new StoredParameter(reader.GetString(0), reader.GetString(1)),
                State: new StoredState(reader.GetString(2),reader.GetString(3)),
                Status: (Status) reader.GetInt32(4),
                Result: new StoredResult(
                    hasResult ? reader.GetString(5) : null, 
                    hasResult ? reader.GetString(6) : null
                ),
                Exception: !hasException ? null : JsonSerializer.Deserialize<StoredException>(reader.GetString(7)),
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
        await using var conn = await CreateConnection();
        var sql = @$"
            DELETE FROM {_tablePrefix}rfunctions
            WHERE function_type_id = $1
            AND function_instance_id = $2 ";
        
        if (expectedEpoch != null)
            sql += "AND epoch = $3";

        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = functionId.TypeId.Value},
                new() {Value = functionId.InstanceId.Value},
            }
        };
        if (expectedEpoch != null)
            command.Parameters.Add(new() { Value = expectedEpoch.Value });

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
}