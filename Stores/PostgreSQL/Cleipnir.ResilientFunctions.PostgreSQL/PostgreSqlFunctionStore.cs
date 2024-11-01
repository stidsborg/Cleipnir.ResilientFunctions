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

    public ITypeStore TypeStore => _typeStore;
    private readonly PostgreSqlTypeStore _typeStore;
    
    private readonly PostgreSqlMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;
    
    private readonly PostgreSqlEffectsStore _effectsStore;
    public IEffectsStore EffectsStore => _effectsStore;

    private readonly PostgreSqlTimeoutStore _timeoutStore;
    public ITimeoutStore TimeoutStore => _timeoutStore;
    
    private readonly ICorrelationStore _correlationStore;
    public ICorrelationStore CorrelationStore => _correlationStore;
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
        _timeoutStore = new PostgreSqlTimeoutStore(connectionString, _tableName);
        _correlationStore = new PostgreSqlCorrelationStore(connectionString, _tableName);
        _typeStore = new PostgreSqlTypeStore(connectionString, _tableName);
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
        await _effectsStore.Initialize();
        await _timeoutStore.Initialize();
        await _correlationStore.Initialize();
        await _typeStore.Initialize();
        await using var conn = await CreateConnection();
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tableName} (
                type INT NOT NULL,
                instance VARCHAR(200) NOT NULL,
                epoch INT NOT NULL DEFAULT 0,
                expires BIGINT NOT NULL,
                interrupted BOOLEAN NOT NULL DEFAULT FALSE,
                param_json BYTEA NULL,            
                status INT NOT NULL DEFAULT {(int) Status.Executing},
                result_json BYTEA NULL,
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
        await _correlationStore.Truncate();
        await _typeStore.Truncate();
        
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {_tableName}";
        await using var command = new NpgsqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _createFunctionSql;
    public async Task<bool> CreateFunction(
        StoredId storedId, 
        byte[]? param, 
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
                new() {Value = storedId.StoredType.Value},
                new() {Value = storedId.Instance},
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
                        new() { Value = idWithParam.StoredId.StoredType.Value },
                        new() { Value = idWithParam.StoredId.Instance },
                        new() { Value = idWithParam.Param == null ? DBNull.Value : idWithParam.Param }
                    }
                };
                batch.BatchCommands.Add(batchCommand);
            }

            await batch.ExecuteNonQueryAsync();
        }
    }

    private string? _restartExecutionSql;
    public async Task<StoredFlow?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration)
    {
        await using var conn = await CreateConnection();

        _restartExecutionSql ??= @$"
            UPDATE {_tableName}
            SET epoch = epoch + 1, status = {(int)Status.Executing}, expires = $1, interrupted = FALSE
            WHERE type = $2 AND instance = $3 AND epoch = $4
            RETURNING               
                param_json, 
                status,
                result_json, 
                exception_json,
                expires,
                epoch, 
                interrupted,
                timestamp";

        await using var command = new NpgsqlCommand(_restartExecutionSql, conn)
        {
            Parameters =
            {
                new() { Value = leaseExpiration },
                new() { Value = storedId.StoredType.Value },
                new() { Value = storedId.Instance },
                new() { Value = expectedEpoch },
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return default;

        var sf = await ReadToStoredFunction(storedId, reader);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    private string? _renewLeaseSql;
    public async Task<bool> RenewLease(StoredId storedId, int expectedEpoch, long leaseExpiration)
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
                new() {Value = storedId.StoredType.Value},
                new() {Value = storedId.Instance},
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
            var flowType = reader.GetInt32(0);
            var flowInstance = reader.GetString(1);
            var epoch = reader.GetInt32(2);
            var flowId = new StoredId(new StoredType(flowType), flowInstance);
            functions.Add(new IdAndEpoch(flowId, epoch));
        }

        return functions;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(StoredType storedType, long completedBefore)
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
                new() {Value = storedType.Value},
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
        StoredId storedId, Status status, 
        byte[]? param, byte[]? result, 
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
                new() {Value = storedId.StoredType.Value},
                new() {Value = storedId.Instance},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _succeedFunctionSql;
    public async Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        _succeedFunctionSql ??= $@"
            UPDATE {_tableName}
            SET status = {(int) Status.Succeeded}, result_json = $1, timestamp = $2
            WHERE 
                type = $3 AND 
                instance = $4 AND 
                epoch = $5";
        await using var command = new NpgsqlCommand(_succeedFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = result == null ? DBNull.Value : result },
                new() { Value = timestamp },
                new() { Value = storedId.StoredType.Value },
                new() { Value = storedId.Instance },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _postponeFunctionSql;
    public async Task<bool> PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        _postponeFunctionSql ??= $@"
            UPDATE {_tableName}
            SET status = {(int) Status.Postponed}, expires = $1, timestamp = $2
            WHERE 
                type = $3 AND 
                instance = $4 AND 
                epoch = $5";
        await using var command = new NpgsqlCommand(_postponeFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = postponeUntil },
                new() { Value = timestamp },
                new() { Value = storedId.StoredType.Value },
                new() { Value = storedId.Instance },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _failFunctionSql;
    public async Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        _failFunctionSql ??= $@"
            UPDATE {_tableName}
            SET status = {(int) Status.Failed}, exception_json = $1, timestamp = $2
            WHERE 
                type = $3 AND 
                instance = $4 AND 
                epoch = $5";
        await using var command = new NpgsqlCommand(_failFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = JsonSerializer.Serialize(storedException) },
                new() { Value = timestamp },
                new() { Value = storedId.StoredType.Value },
                new() { Value = storedId.Instance },
                new() { Value = expectedEpoch },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _suspendFunctionSql;
    public async Task<bool> SuspendFunction(
        StoredId storedId, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();

        _suspendFunctionSql ??= $@"
            UPDATE {_tableName}
            SET status = {(int)Status.Suspended}, timestamp = $1
            WHERE type = $2 AND 
                  instance = $3 AND 
                  epoch = $4 AND
                  NOT interrupted;";
        await using var command = new NpgsqlCommand(_suspendFunctionSql, conn)
        {
            Parameters =
            {
                new() { Value = timestamp },
                new() { Value = storedId.StoredType.Value },
                new() { Value = storedId.Instance },
                new() { Value = expectedEpoch }
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        StoredId storedId,
        byte[]? param, byte[]? result,
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
                new() { Value = storedId.StoredType.Value },
                new() { Value = storedId.Instance },
                new() { Value = expectedEpoch },
            }
        };

        await using var _ = command;
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _interruptSql;
    private string? _interruptSqlIfExecuting;
    public async Task<bool> Interrupt(StoredId storedId, bool onlyIfExecuting)
    {
        await using var conn = await CreateConnection();

        _interruptSql ??= $@"
                UPDATE {_tableName}
                SET 
                    interrupted = TRUE,
                    status = 
                        CASE 
                            WHEN status = {(int) Status.Suspended} THEN {(int) Status.Postponed}
                            ELSE status
                        END,
                    expires = 
                        CASE
                            WHEN status = {(int) Status.Postponed} THEN 0
                            WHEN status = {(int) Status.Suspended} THEN 0
                            ELSE expires
                        END
                WHERE type = $1 AND instance = $2";
        _interruptSqlIfExecuting ??= _interruptSql + $" AND status = {(int) Status.Executing}";

        var sql = onlyIfExecuting
            ? _interruptSqlIfExecuting
            : _interruptSql;
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.StoredType.Value },
                new() { Value = storedId.Instance },
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _interruptedSql;
    public async Task<bool?> Interrupted(StoredId storedId)
    {
        await using var conn = await CreateConnection();

        _interruptedSql ??= $@"
                SELECT interrupted 
                FROM {_tableName}
                WHERE type = $1 AND instance = $2";
        await using var command = new NpgsqlCommand(_interruptedSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.StoredType.Value },
                new() { Value = storedId.Instance },
            }
        };
        return (bool?) await command.ExecuteScalarAsync();
    }

    private string? _getFunctionStatusSql;
    public async Task<StatusAndEpoch?> GetFunctionStatus(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _getFunctionStatusSql ??= $@"
            SELECT status, epoch
            FROM {_tableName}
            WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_getFunctionStatusSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.StoredType.Value},
                new() {Value = storedId.Instance}
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
    public async Task<StoredFlow?> GetFunction(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _getFunctionSql ??= $@"
            SELECT               
                param_json,             
                status,
                result_json,         
                exception_json,
                expires,
                epoch, 
                interrupted,
                timestamp
            FROM {_tableName}
            WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_getFunctionSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.StoredType.Value},
                new() {Value = storedId.Instance}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(storedId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<FlowInstance>> GetInstances(StoredType storedType, Status status)
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
                new() {Value = storedType.Value},
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
    public async Task<IReadOnlyList<FlowInstance>> GetInstances(StoredType storedType)
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
                new() {Value = storedType.Value},
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
    public async Task<IReadOnlyList<StoredType>> GetTypes()
    {
        await using var conn = await CreateConnection();
        
        _getTypesSql ??= $"SELECT DISTINCT(type) FROM {_tableName}";
        await using var command = new NpgsqlCommand(_getTypesSql, conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        var flowTypes = new List<StoredType>();
        while (await reader.ReadAsync())
        {
            var flowType = reader.GetInt32(0);
            flowTypes.Add(new StoredType(flowType));
        }

        return flowTypes;
    }

    private async Task<StoredFlow?> ReadToStoredFunction(StoredId storedId, NpgsqlDataReader reader)
    {
        /*
           0  param_json,         
           1  status,
           2  result_json,         
           3  exception_json,
           4  expires,
           5  epoch,         
           6 interrupted,
           7 timestamp
         */
        while (await reader.ReadAsync())
        {
            var hasParameter = !await reader.IsDBNullAsync(0);
            var hasResult = !await reader.IsDBNullAsync(2);
            var hasException = !await reader.IsDBNullAsync(3);
            
            return new StoredFlow(
                storedId,
                hasParameter ? (byte[]) reader.GetValue(0) : null,
                Status: (Status) reader.GetInt32(1),
                Result: hasResult ? (byte[]) reader.GetValue(2) : null, 
                Exception: !hasException ? null : JsonSerializer.Deserialize<StoredException>(reader.GetString(3)),
                Expires: reader.GetInt64(4),
                Epoch: reader.GetInt32(5),
                Interrupted: reader.GetBoolean(6),
                Timestamp: reader.GetInt64(7)
            );
        }

        return null;
    }
    
    public async Task<bool> DeleteFunction(StoredId storedId)
    {
        await _messageStore.Truncate(storedId);
        await _effectsStore.Remove(storedId);
        await _timeoutStore.Remove(storedId);
        await _correlationStore.RemoveCorrelations(storedId);

        return await DeleteStoredFunction(storedId);
    }

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(StoredId storedId)
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
                new() {Value = storedId.StoredType.Value},
                new() {Value = storedId.Instance},
            }
        };
       
        return await command.ExecuteNonQueryAsync() == 1;
    }
}