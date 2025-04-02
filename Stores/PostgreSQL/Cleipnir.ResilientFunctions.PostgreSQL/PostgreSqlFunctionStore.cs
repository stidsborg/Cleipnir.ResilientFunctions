using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
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
    
    private readonly PostgreSqlSemaphoreStore _semaphoreStore;
    public ISemaphoreStore SemaphoreStore => _semaphoreStore;

    public Utilities Utilities { get; }
    public IMigrator Migrator => _migrator;
    private readonly PostgreSqlMigrator _migrator;
    
    private readonly PostgresSqlUnderlyingRegister _postgresSqlUnderlyingRegister;
    private readonly SqlGenerator _sqlGenerator;

    public PostgreSqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tableName = tablePrefix == "" ? "rfunctions" : tablePrefix;
        _connectionString = connectionString;
        _sqlGenerator = new SqlGenerator(_tableName);
        
        _messageStore = new PostgreSqlMessageStore(connectionString, _sqlGenerator, _tableName);
        _effectsStore = new PostgreSqlEffectsStore(connectionString, _sqlGenerator, _tableName);
        _timeoutStore = new PostgreSqlTimeoutStore(connectionString, _tableName);
        _correlationStore = new PostgreSqlCorrelationStore(connectionString, _tableName);
        _semaphoreStore = new PostgreSqlSemaphoreStore(connectionString, _tableName);
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
        await _semaphoreStore.Initialize();
        await _typeStore.Initialize();
        await using var conn = await CreateConnection();
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tableName} (
                type INT NOT NULL,
                instance UUID NOT NULL,
                epoch INT NOT NULL DEFAULT 0,
                expires BIGINT NOT NULL,
                interrupted BOOLEAN NOT NULL DEFAULT FALSE,
                param_json BYTEA NULL,            
                status INT NOT NULL DEFAULT {(int) Status.Executing},
                result_json BYTEA NULL,
                exception_json TEXT NULL,                                
                timestamp BIGINT NOT NULL,
                human_instance_id TEXT NOT NULL,
                parent TEXT NULL,
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
        await _semaphoreStore.Truncate();
        
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {_tableName}";
        await using var command = new NpgsqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    public async Task<bool> CreateFunction(
        StoredId storedId,
        FlowInstance humanInstanceId,
        byte[]? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent,
        IReadOnlyList<StoredEffect>? effects = null, 
        IReadOnlyList<StoredMessage>? messages = null
        )
    {
        if (effects == null && messages == null)
        {
            await using var conn = await CreateConnection();
            await using var command = _sqlGenerator.CreateFunction(
                storedId,
                humanInstanceId,
                param,
                leaseExpiration,
                postponeUntil,
                timestamp,
                parent,
                ignoreConflict: true
            ).ToNpgsqlCommand(conn);

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows == 1;    
        }

        try
        {
            var commands = new List<StoreCommand>();
            var createCommand = _sqlGenerator.CreateFunction(
                storedId,
                humanInstanceId,
                param,
                leaseExpiration,
                postponeUntil,
                timestamp,
                parent,
                ignoreConflict: false
            );
            commands.Add(createCommand);

            if (effects?.Any() ?? false)
                commands.AddRange(
                    _sqlGenerator.UpdateEffects(
                        effects.Select(e => new StoredEffectChange(storedId, e.StoredEffectId, CrudOperation.Insert, e)).ToList()
                    )
                );

            if (messages?.Any() ?? false)
                commands.AddRange(_sqlGenerator.AppendMessages(
                        messages.Select((msg, position) => new StoredIdAndMessageWithPosition(storedId, msg, position)).ToList()
                    )
                );

            await using var batch = commands.ToNpgsqlBatch();
            await using var conn = await CreateConnection();
            batch.WithConnection(conn);
            await batch.ExecuteNonQueryAsync();
            return true;
        }
        catch (PostgresException e) when (e.SqlState == "23505")
        {
            return false;
        }
    }

    private string? _bulkScheduleFunctionsSql;
    public async Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
    {
        _bulkScheduleFunctionsSql ??= @$"
            INSERT INTO {_tableName}
                (type, instance, status, param_json, expires, timestamp, human_instance_id, parent)
            VALUES
                ($1, $2, {(int) Status.Postponed}, $3, 0, 0, $4, $5)
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
                        new() { Value = idWithParam.StoredId.Type.Value },
                        new() { Value = idWithParam.StoredId.Instance.Value },
                        new() { Value = idWithParam.Param == null ? DBNull.Value : idWithParam.Param },
                        new() { Value = idWithParam.HumanInstanceId },
                        new() { Value = parent?.Serialize() ?? (object) DBNull.Value },
                    }
                };
                batch.BatchCommands.Add(batchCommand);
            }

            await batch.ExecuteNonQueryAsync();
        }
    }

    public async Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration)
    {
        var restartCommand = _sqlGenerator.RestartExecution(storedId, expectedEpoch, leaseExpiration);
        var effectsCommand = _sqlGenerator.GetEffects(storedId);
        var messagesCommand = _sqlGenerator.GetMessages(storedId, skip: 0);
        
        await using var conn = await CreateConnection();
        await using var command = StoreCommands
            .CreateBatch(restartCommand, effectsCommand, messagesCommand)
            .WithConnection(conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        
        var sf = await _sqlGenerator.ReadFunction(storedId, reader);
        if (sf?.Epoch != expectedEpoch + 1)
            return null;
        await reader.NextResultAsync();
        var effects = await _sqlGenerator.ReadEffects(reader);
        await reader.NextResultAsync();
        
        var messages = await _sqlGenerator.ReadMessages(reader);
        return new StoredFlowWithEffectsAndMessages(sf, effects, messages);
    }

    public async Task<int> RenewLeases(IReadOnlyList<LeaseUpdate> leaseUpdates, long leaseExpiration)
    {
        var predicates = leaseUpdates
            .Select(u =>
                $"(type = {u.StoredId.Type.Value} AND instance = '{u.StoredId.Instance.Value}' AND epoch = {u.ExpectedEpoch})"
            ).StringJoin(" OR " + Environment.NewLine);
        
        var sql = $@"
            UPDATE {_tableName}
            SET expires = {leaseExpiration}
            WHERE {predicates}";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);

        return await command.ExecuteNonQueryAsync();
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
            var type = reader.GetInt32(0);
            var instance = reader.GetGuid(1).ToStoredInstance();
            var epoch = reader.GetInt32(2);
            var flowId = new StoredId(new StoredType(type), instance);
            functions.Add(new IdAndEpoch(flowId, epoch));
        }

        return functions;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<StoredInstance>> GetSucceededFunctions(StoredType storedType, long completedBefore)
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
        var flowInstances = new List<StoredInstance>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetGuid(0).ToStoredInstance();
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
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value},
                new() {Value = expectedEpoch},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch, 
        IReadOnlyList<StoredEffectChange>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.SucceedFunction(
            storedId,
            result,
            timestamp,
            expectedEpoch
        ).ToNpgsqlCommand(conn);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        bool ignoreInterrupted,
        int expectedEpoch, 
        IReadOnlyList<StoredEffectChange>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.PostponeFunction(
            storedId,
            postponeUntil,
            timestamp,
            ignoreInterrupted,
            expectedEpoch
        ).ToNpgsqlCommand(conn);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch, 
        IReadOnlyList<StoredEffectChange>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.FailFunction(
            storedId,
            storedException,
            timestamp,
            expectedEpoch
        ).ToNpgsqlCommand(conn);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> SuspendFunction(
        StoredId storedId, 
        long timestamp,
        int expectedEpoch, 
        IReadOnlyList<StoredEffectChange>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();

        await using var command = _sqlGenerator
            .SuspendFunction(storedId, timestamp, expectedEpoch)
            .ToNpgsqlCommand(conn);
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
        
        await using var command = new NpgsqlCommand(_setParametersSql, conn)
        {
            Parameters =
            {
                new() { Value = param ?? (object) DBNull.Value },
                new() { Value = result ?? (object) DBNull.Value },
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value },
                new() { Value = expectedEpoch },
            }
        };
        
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
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value },
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task Interrupt(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return;
        
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.Interrupt(storedIds).ToNpgsqlCommand(conn);
        await command.ExecuteNonQueryAsync();
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
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value },
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
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value}
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

    public async Task<IReadOnlyList<StatusAndEpochWithId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
    {
        var predicates = storedIds
            .Select(s => new { Type = s.Type.Value, Instance = s.Instance.Value })
            .GroupBy(id => id.Type, id => id.Instance)
            .Select(g => $"(type = {g.Key} AND instance IN ({string.Join(",", g.Select(instance => $"'{instance}'"))}))")
            .StringJoin(" OR " + Environment.NewLine);

        var sql = @$"
            SELECT type, instance, status, epoch, expires
            FROM {_tableName}
            WHERE {predicates}";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);
        
        var toReturn = new List<StatusAndEpochWithId>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var type = reader.GetInt32(0).ToStoredType();
            var instance = reader.GetGuid(1).ToStoredInstance();
            var status = (Status) reader.GetInt32(2);
            var epoch = reader.GetInt32(3);
            var expires = reader.GetInt64(4);

            var storedId = new StoredId(type, instance);
            toReturn.Add(new StatusAndEpochWithId(storedId, status, epoch, expires));
        }
        
        return toReturn;
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
                timestamp,
                human_instance_id,
                parent
            FROM {_tableName}
            WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_getFunctionSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(storedId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType, Status status)
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
        var instances = new List<StoredInstance>();
        while (await reader.ReadAsync())
        {
            var instance = reader.GetGuid(0).ToStoredInstance();
            instances.Add(instance);
        }

        return instances;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType)
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
        var instances = new List<StoredInstance>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetGuid(0).ToStoredInstance();
            instances.Add(flowInstance);
        }

        return instances;
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
           7 timestamp,
           8 human_instance_id
           9 parent
         */
        while (await reader.ReadAsync())
        {
            var hasParameter = !await reader.IsDBNullAsync(0);
            var hasResult = !await reader.IsDBNullAsync(2);
            var hasException = !await reader.IsDBNullAsync(3);
            var hasParent = !await reader.IsDBNullAsync(9);
            
            return new StoredFlow(
                storedId,
                HumanInstanceId: reader.GetString(8),
                hasParameter ? (byte[]) reader.GetValue(0) : null,
                Status: (Status) reader.GetInt32(1),
                Result: hasResult ? (byte[]) reader.GetValue(2) : null, 
                Exception: !hasException ? null : JsonSerializer.Deserialize<StoredException>(reader.GetString(3)),
                Expires: reader.GetInt64(4),
                Epoch: reader.GetInt32(5),
                Interrupted: reader.GetBoolean(6),
                Timestamp: reader.GetInt64(7),
                ParentId: hasParent ? StoredId.Deserialize(reader.GetString(9)) : null
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
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value},
            }
        };
       
        return await command.ExecuteNonQueryAsync() == 1;
    }
}