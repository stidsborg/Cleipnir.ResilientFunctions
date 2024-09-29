using System;
using System.Collections.Generic;
using System.Linq;
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
    private readonly string _tableName;

    private readonly PostgreSqlMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;

    private readonly PostgreSqlStatesStore _statesStore;
    public IStatesStore StatesStore => _statesStore;
    
    private readonly PostgreSqlEffectsStore _effectsStore;
    public IEffectsStore EffectsStore => _effectsStore;

    private readonly PostgreSqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    
    private readonly ICorrelationStore _correlationStore;
    public ICorrelationStore CorrelationStore => _correlationStore;
    private readonly PostgreSqlReplcaStore _replicaStore;
    public IReplicaStore ReplicaStore => _replicaStore;
    public Utilities Utilities { get; }
    public IMigrator Migrator => _migrator;
    private readonly PostgreSqlMigrator _migrator;
    
    private readonly PostgresSqlUnderlyingRegister _postgresSqlUnderlyingRegister;

    public PostgreSqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tableName = tablePrefix == "" ? "rfunctions" : tablePrefix;
        _connectionString = connectionString;
        
        _messageStore = new PostgreSqlMessageStore(connectionString, _tableName);
        _effectsStore = new PostgreSqlEffectsStore(connectionString, _tableName);
        _statesStore = new PostgreSqlStatesStore(connectionString, _tableName);
        _timeoutStore = new PostgreSqlTimeoutStore(connectionString, _tableName);
        _correlationStore = new PostgreSqlCorrelationStore(connectionString, _tableName);
        _replicaStore = new PostgreSqlReplcaStore(connectionString, _tableName);
        _postgresSqlUnderlyingRegister = new PostgresSqlUnderlyingRegister(connectionString, _tableName);
        _migrator = new PostgreSqlMigrator(connectionString, _tableName);
        Utilities = new Utilities(_postgresSqlUnderlyingRegister);
    } 

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        var createTables = await _migrator.InitializeAndMigrate();
        if (!createTables)
            return;
        
        await _postgresSqlUnderlyingRegister.Initialize();
        await _messageStore.Initialize();
        await _statesStore.Initialize();
        await _effectsStore.Initialize();
        await _timeoutStore.Initialize();
        await _correlationStore.Initialize();
        await _replicaStore.Initialize();
        await using var conn = await CreateConnection();
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tableName} (
                type VARCHAR(200) NOT NULL,
                instance VARCHAR(200) NOT NULL,
                epoch INT NOT NULL DEFAULT 0,
                expires BIGINT NOT NULL,
                interrupt_count BIGINT NOT NULL DEFAULT 0,
                param_json TEXT NULL,            
                status INT NOT NULL DEFAULT {(int) Status.Executing},
                result_json TEXT NULL,
                default_state TEXT NULL,
                exception_json TEXT NULL,                                
                timestamp BIGINT NOT NULL,
                PRIMARY KEY (type, instance)
            );
            CREATE INDEX IF NOT EXISTS idx_{_tableName}_expires
            ON {_tableName}(expires, type, instance)
            INCLUDE (epoch)
            WHERE status = {(int) Status.Executing} OR status = {(int) Status.Postponed};           

            CREATE INDEX IF NOT EXISTS idx_{_tableName}_succeeded
            ON {_tableName}(type, instance)
            WHERE status = {(int) Status.Succeeded};
            ";

        await using var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private string? _truncateTableSql;
    public async Task TruncateTables()
    {
        await _messageStore.TruncateTable();
        await _timeoutStore.Truncate();
        await _postgresSqlUnderlyingRegister.TruncateTable();
        await _effectsStore.Truncate();
        await _statesStore.Truncate();
        await _correlationStore.Truncate();
        await _replicaStore.Truncate();
        
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {_tableName}";
        await using var command = new NpgsqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _createFunctionSql;
    public async Task<bool> CreateFunction(
        FlowId flowId, 
        string? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        await using var conn = await CreateConnection();
        
        _createFunctionSql ??= @$"
            INSERT INTO {_tableName}
                (type, instance, status, param_json, expires, timestamp)
            VALUES
                ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING;";
        await using var command = new NpgsqlCommand(_createFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = (int) (postponeUntil == null ? Status.Executing : Status.Postponed)},
                new() {Value = param == null ? DBNull.Value : param},
                new() {Value = postponeUntil ?? leaseExpiration},
                new() {Value = timestamp}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _bulkScheduleFunctionsSql;
    public async Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam)
    {
        _bulkScheduleFunctionsSql ??= @$"
            INSERT INTO {_tableName}
                (type, instance, status, param_json, expires, timestamp)
            VALUES
                ($1, $2, {(int) Status.Postponed}, $3, 0, 0)
            ON CONFLICT DO NOTHING;";

        await using var conn = await CreateConnection();
        var chunks = functionsWithParam.Chunk(100);
        foreach (var chunk in chunks)
        {
            await using var batch = new NpgsqlBatch(conn);
            foreach (var idWithParam in chunk)
            {
                var batchCommand = new NpgsqlBatchCommand(_bulkScheduleFunctionsSql)
                {
                    Parameters =
                    {
                        new() { Value = idWithParam.FlowId.Type.Value },
                        new() { Value = idWithParam.FlowId.Instance.Value },
                        new() { Value = idWithParam.Param == null ? DBNull.Value : idWithParam.Param }
                    }
                };
                batch.BatchCommands.Add(batchCommand);
            }

            await batch.ExecuteNonQueryAsync();
        }
    }

    private string? _restartExecutionSql;
    public async Task<StoredFlow?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateConnection();

        _restartExecutionSql ??= @$"
            UPDATE {_tableName}
            SET epoch = epoch + 1, status = {(int)Status.Executing}, expires = $1
            WHERE type = $2 AND instance = $3 AND epoch = $4
            RETURNING               
                param_json, 
                status,
                result_json, 
                default_state,
                exception_json,
                expires,
                epoch, 
                interrupt_count,
                timestamp";

        await using var command = new NpgsqlCommand(_restartExecutionSql, conn)
        {
            Parameters =
            {
                new() { Value = leaseExpiration },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;

        var sf = await ReadToStoredFunction(flowId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    private string? _renewLeaseSql;
    public async Task<bool> RenewLease(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateConnection();
        _renewLeaseSql ??= $@"
            UPDATE {_tableName}
            SET expires = $1
            WHERE type = $2 AND instance = $3 AND epoch = $4";
        await using var command = new NpgsqlCommand(_renewLeaseSql, conn)
        {
            Parameters =
            {
                new() {Value = leaseExpiration},
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    private string? _getExpiredFunctionsSql;
    public async Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore)
    {
        await using var conn = await CreateConnection();
        _getExpiredFunctionsSql ??= @$"
            SELECT type, instance, epoch
            FROM {_tableName}
            WHERE expires <= $1 AND (status = {(int) Status.Postponed} OR status = {(int) Status.Executing})";
        await using var command = new NpgsqlCommand(_getExpiredFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = expiresBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var functions = new List<IdAndEpoch>();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetString(0);
            var flowInstance = reader.GetString(1);
            var epoch = reader.GetInt32(2);
            var flowId = new FlowId(flowType, flowInstance);
            functions.Add(new IdAndEpoch(flowId, epoch));
        }

        return functions;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(FlowType flowType, long completedBefore)
    {
        await using var conn = await CreateConnection();
        _getSucceededFunctionsSql ??= @$"
            SELECT instance
            FROM {_tableName}
            WHERE type = $1 AND status = {(int) Status.Succeeded} AND timestamp <= $2";
        await using var command = new NpgsqlCommand(_getSucceededFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() {Value = completedBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var flowInstances = new List<FlowInstance>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetString(0);
            flowInstances.Add(flowInstance);
        }

        return flowInstances;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        FlowId flowId, Status status, 
        string? param, string? result, 
        StoredException? storedException, 
        long expires,
        int expectedEpoch)
    {
        await using var conn = await CreateConnection();
       
        _setFunctionStateSql ??= $@"
            UPDATE {_tableName}
            SET status = $1,
                param_json = $2, 
                result_json = $3, 
                exception_json = $4, expires = $5,
                epoch = epoch + 1
            WHERE 
                type = $6 AND 
                instance = $7 AND 
                epoch = $8";
        await using var command = new NpgsqlCommand(_setFunctionStateSql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = param == null ? DBNull.Value : param},
                new() {Value = result == null ? DBNull.Value : result},
                new() {Value = storedException == null ? DBNull.Value : JsonSerializer.Serialize(storedException)},
                new() {Value = expires },
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _succeedFunctionSql;
    public async Task<bool> SucceedFunction(
        FlowId flowId, 
        string? result, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        _succeedFunctionSql ??= $@"
            UPDATE {_tableName}
            SET status = {(int) Status.Succeeded}, result_json = $1, default_state = $2, timestamp = $3
            WHERE 
                type = $4 AND 
                instance = $5 AND 
                epoch = $6";
        await using var command = new NpgsqlCommand(_succeedFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = result == null ? DBNull.Value : result },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _postponeFunctionSql;
    public async Task<bool> PostponeFunction(
        FlowId flowId, 
        long postponeUntil, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        _postponeFunctionSql ??= $@"
            UPDATE {_tableName}
            SET status = {(int) Status.Postponed}, expires = $1, default_state = $2, timestamp = $3
            WHERE 
                type = $4 AND 
                instance = $5 AND 
                epoch = $6";
        await using var command = new NpgsqlCommand(_postponeFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = postponeUntil },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _failFunctionSql;
    public async Task<bool> FailFunction(
        FlowId flowId, 
        StoredException storedException, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        _failFunctionSql ??= $@"
            UPDATE {_tableName}
            SET status = {(int) Status.Failed}, exception_json = $1, default_state = $2, timestamp = $3
            WHERE 
                type = $4 AND 
                instance = $5 AND 
                epoch = $6";
        await using var command = new NpgsqlCommand(_failFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = JsonSerializer.Serialize(storedException) },
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _suspendFunctionSql;
    public async Task<bool> SuspendFunction(
        FlowId flowId, 
        long expectedInterruptCount, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();

        _suspendFunctionSql ??= $@"
            UPDATE {_tableName}
            SET status = {(int)Status.Suspended}, default_state = $1, timestamp = $2
            WHERE type = $3 AND 
                  instance = $4 AND 
                  epoch = $5 AND
                  interrupt_count = $6";
        await using var command = new NpgsqlCommand(_suspendFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = defaultState ?? (object) DBNull.Value },
                new() { Value = timestamp },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
                new() { Value = expectedInterruptCount },
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _setDefaultStateSql;
    public async Task SetDefaultState(FlowId flowId, string? stateJson)
    {
        await using var conn = await CreateConnection();
        _setDefaultStateSql ??= $@"
            UPDATE {_tableName}
            SET default_state = $1
            WHERE type = $2 AND instance = $3";
        await using var command = new NpgsqlCommand(_setDefaultStateSql, conn)
        {
            Parameters =
            {
                new() {Value = stateJson ?? (object) DBNull.Value},
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        FlowId flowId,
        string? param, string? result,
        int expectedEpoch)
    {
        await using var conn = await CreateConnection();
        
        _setParametersSql ??= $@"
            UPDATE {_tableName}
            SET param_json = $1,             
                result_json = $2, 
                epoch = epoch + 1
            WHERE type = $3 AND instance = $4 AND epoch = $5";
        
        var command = new NpgsqlCommand(_setParametersSql, conn)
        {
            Parameters =
            {
                new() { Value = param ?? (object) DBNull.Value },
                new() { Value = result ?? (object) DBNull.Value },
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };

        await using var _ = command;
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _incrementInterruptCountSql;
    public async Task<bool> IncrementInterruptCount(FlowId flowId)
    {
        await using var conn = await CreateConnection();

        _incrementInterruptCountSql ??= $@"
                UPDATE {_tableName}
                SET interrupt_count = interrupt_count + 1
                WHERE type = $1 AND instance = $2  AND status = {(int) Status.Executing};";
        await using var command = new NpgsqlCommand(_incrementInterruptCountSql, conn)
        {
            Parameters =
            {
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getInterruptCountSql;
    public async Task<long?> GetInterruptCount(FlowId flowId)
    {
        await using var conn = await CreateConnection();

        _getInterruptCountSql ??= $@"
                SELECT interrupt_count 
                FROM {_tableName}
                WHERE type = $1 AND instance = $2";
        await using var command = new NpgsqlCommand(_getInterruptCountSql, conn)
        {
            Parameters =
            {
                new() { Value = flowId.Type.Value },
                new() { Value = flowId.Instance.Value },
            }
        };
        return (long?) await command.ExecuteScalarAsync();
    }

    private string? _getFunctionStatusSql;
    public async Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _getFunctionStatusSql ??= $@"
            SELECT status, epoch
            FROM {_tableName}
            WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_getFunctionStatusSql, conn)
        {
            Parameters = { 
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
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

    private string? _getFunctionSql;
    public async Task<StoredFlow?> GetFunction(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _getFunctionSql ??= $@"
            SELECT               
                param_json,             
                status,
                result_json,         
                default_state,
                exception_json,
                expires,
                epoch, 
                interrupt_count,
                timestamp
            FROM {_tableName}
            WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_getFunctionSql, conn)
        {
            Parameters = { 
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(flowId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType, Status status)
    {
        await using var conn = await CreateConnection();
        _getInstancesWithStatusSql ??= @$"
            SELECT instance
            FROM {_tableName}
            WHERE type = $1 AND status = $2";
        
        await using var command = new NpgsqlCommand(_getInstancesWithStatusSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
                new() {Value = (int) status},
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<FlowInstance>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetString(0);
            instances.Add(flowInstance);
        }

        return instances;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType)
    {
        await using var conn = await CreateConnection();
        
        _getInstancesSql ??= @$"
            SELECT instance
            FROM {_tableName}
            WHERE type = $1";
        
        await using var command = new NpgsqlCommand(_getInstancesSql, conn)
        {
            Parameters =
            {
                new() {Value = flowType.Value},
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var instances = new List<FlowInstance>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetString(0);
            instances.Add(flowInstance);
        }

        return instances;
    }

    private string? _getTypesSql;
    public async Task<IReadOnlyList<FlowType>> GetTypes()
    {
        await using var conn = await CreateConnection();
        
        _getTypesSql ??= $"SELECT DISTINCT(type) FROM {_tableName}";
        await using var command = new NpgsqlCommand(_getTypesSql, conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        var flowTypes = new List<FlowType>();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetString(0);
            flowTypes.Add(flowType);
        }

        return flowTypes;
    }

    private async Task<StoredFlow?> ReadToStoredFunction(FlowId flowId, NpgsqlDataReader reader)
    {
        /*
           0  param_json,         
           1  status,
           2  result_json,         
           3  default_state
           4  exception_json,
           5  expires,
           6  epoch,         
           7 interrupt_count,
           8 timestamp
         */
        while (await reader.ReadAsync())
        {
            var hasParameter = !await reader.IsDBNullAsync(0);
            var hasResult = !await reader.IsDBNullAsync(2);
            var hasDefaultState = !await reader.IsDBNullAsync(3);
            var hasException = !await reader.IsDBNullAsync(4);
            
            return new StoredFlow(
                flowId,
                hasParameter ? reader.GetString(0) : null,
                Status: (Status) reader.GetInt32(1),
                Result: hasResult ? reader.GetString(2) : null, 
                DefaultState: hasDefaultState ? reader.GetString(3) : null,
                Exception: !hasException ? null : JsonSerializer.Deserialize<StoredException>(reader.GetString(4)),
                Expires: reader.GetInt64(5),
                Epoch: reader.GetInt32(6),
                InterruptCount: reader.GetInt64(7),
                Timestamp: reader.GetInt64(8)
            );
        }

        return null;
    }
    
    public async Task<bool> DeleteFunction(FlowId flowId)
    {
        await _messageStore.Truncate(flowId);
        await _effectsStore.Remove(flowId);
        await _statesStore.Remove(flowId);
        await _timeoutStore.Remove(flowId);
        await _correlationStore.RemoveCorrelations(flowId);

        return await DeleteStoredFunction(flowId);
    }

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _deleteFunctionSql ??= @$"
            DELETE FROM {_tableName}
            WHERE type = $1
            AND instance = $2 ";

        await using var command = new NpgsqlCommand(_deleteFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = flowId.Type.Value},
                new() {Value = flowId.Instance.Value},
            }
        };
       
        return await command.ExecuteNonQueryAsync() == 1;
    }
}