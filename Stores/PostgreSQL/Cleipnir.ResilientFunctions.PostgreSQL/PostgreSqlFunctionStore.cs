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
    
    private readonly ICorrelationStore _correlationStore;
    public ICorrelationStore CorrelationStore => _correlationStore;
    
    private readonly PostgreSqlSemaphoreStore _semaphoreStore;
    public ISemaphoreStore SemaphoreStore => _semaphoreStore;
    private readonly PostgreSqlDbReplicaStore _replicaStore;
    public IReplicaStore ReplicaStore => _replicaStore;

    public Utilities Utilities { get; }
    
    private readonly PostgresSqlUnderlyingRegister _postgresSqlUnderlyingRegister;
    private readonly SqlGenerator _sqlGenerator;

    public PostgreSqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tableName = tablePrefix == "" ? "rfunctions" : tablePrefix;
        _connectionString = connectionString;
        _sqlGenerator = new SqlGenerator(_tableName);
        
        _messageStore = new PostgreSqlMessageStore(connectionString, _sqlGenerator, _tableName);
        _effectsStore = new PostgreSqlEffectsStore(connectionString, _sqlGenerator, _tableName);
        _correlationStore = new PostgreSqlCorrelationStore(connectionString, _tableName);
        _semaphoreStore = new PostgreSqlSemaphoreStore(connectionString, _tableName);
        _typeStore = new PostgreSqlTypeStore(connectionString, _tableName);
        _postgresSqlUnderlyingRegister = new PostgresSqlUnderlyingRegister(connectionString, _tableName);
        _replicaStore = new PostgreSqlDbReplicaStore(connectionString, _tableName);
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
        if (await DoTablesAlreadyExist())
            return;
        
        await _postgresSqlUnderlyingRegister.Initialize();
        await _messageStore.Initialize();
        await _effectsStore.Initialize();
        await _correlationStore.Initialize();
        await _semaphoreStore.Initialize();
        await _typeStore.Initialize();
        await _replicaStore.Initialize();
        await using var conn = await CreateConnection();
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tableName} (
                id UUID PRIMARY KEY,              
                expires BIGINT NOT NULL,
                interrupted BOOLEAN NOT NULL DEFAULT FALSE,
                param_json BYTEA NULL,            
                status INT NOT NULL DEFAULT {(int) Status.Executing},
                result_json BYTEA NULL,
                exception_json TEXT NULL,                                
                timestamp BIGINT NOT NULL,
                human_instance_id TEXT NOT NULL,
                parent TEXT NULL,
                owner UUID NULL
            );
            CREATE INDEX IF NOT EXISTS idx_{_tableName}_expires
            ON {_tableName}(expires, id)
            INCLUDE (owner)
            WHERE status = {(int) Status.Executing} OR status = {(int) Status.Postponed};           

            CREATE INDEX IF NOT EXISTS idx_{_tableName}_succeeded
            ON {_tableName}(id)
            WHERE status = {(int) Status.Succeeded};";

        await using var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private string? _truncateTableSql;
    public async Task TruncateTables()
    {
        await _messageStore.TruncateTable();
        await _postgresSqlUnderlyingRegister.TruncateTable();
        await _effectsStore.Truncate();
        await _correlationStore.Truncate();
        await _typeStore.Truncate();
        await _semaphoreStore.Truncate();
        await _replicaStore.Truncate();
        
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
        ReplicaId? owner,
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
                owner,
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
                owner,
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
                (id, status, param_json, expires, timestamp, human_instance_id, parent)
            VALUES
                ($1, {(int) Status.Postponed}, $2, 0, 0, $3, $4)
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
                        new() { Value = idWithParam.StoredId.AsGuid },
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

    public async Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, ReplicaId replicaId)
    {
        var restartCommand = _sqlGenerator.RestartExecution(storedId, replicaId);
        var effectsCommand = _sqlGenerator.GetEffects(storedId);
        var messagesCommand = _sqlGenerator.GetMessages(storedId, skip: 0);
        
        await using var conn = await CreateConnection();
        await using var command = StoreCommands
            .CreateBatch(restartCommand, effectsCommand, messagesCommand)
            .WithConnection(conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        
        var sf = await _sqlGenerator.ReadFunction(storedId, reader);
        if (sf?.OwnerId != replicaId)
            return null;
        await reader.NextResultAsync();
        var effects = await _sqlGenerator.ReadEffects(reader);
        await reader.NextResultAsync();
        
        var messages = await _sqlGenerator.ReadMessages(reader);
        return new StoredFlowWithEffectsAndMessages(sf, effects, messages);
    }

    private string? _getExpiredFunctionsSql;
    public async Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long expiresBefore)
    {
        await using var conn = await CreateConnection();
        _getExpiredFunctionsSql ??= @$"
            SELECT id
            FROM {_tableName}
            WHERE expires <= $1 AND status = {(int) Status.Postponed}";
        await using var command = new NpgsqlCommand(_getExpiredFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = expiresBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var instance = reader.GetGuid(0);
            ids.Add(new StoredId(instance));
        }

        return ids;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<StoredId>> GetSucceededFunctions(long completedBefore)
    {
        await using var conn = await CreateConnection();
        _getSucceededFunctionsSql ??= @$"
            SELECT id
            FROM {_tableName}
            WHERE status = {(int) Status.Succeeded} AND timestamp <= $1";
        await using var command = new NpgsqlCommand(_getSucceededFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = completedBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetGuid(0).ToStoredId();
            ids.Add(flowInstance);
        }

        return ids;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        StoredId storedId, Status status, 
        byte[]? param, byte[]? result, 
        StoredException? storedException, 
        long expires,
        ReplicaId? expectedReplica)
    {
        await using var conn = await CreateConnection();
       
        _setFunctionStateSql ??= $@"
            UPDATE {_tableName}
            SET status = $1,
                param_json = $2, 
                result_json = $3, 
                exception_json = $4, expires = $5
            WHERE id = $6";

        var sql = expectedReplica == null
            ? _setFunctionStateSql + " AND owner IS NULL;"
            : _setFunctionStateSql + $" AND owner = '{expectedReplica.AsGuid}'";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = param == null ? DBNull.Value : param},
                new() {Value = result == null ? DBNull.Value : result},
                new() {Value = storedException == null ? DBNull.Value : JsonSerializer.Serialize(storedException)},
                new() {Value = expires },
                new() {Value = storedId.AsGuid}
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        ReplicaId expectedReplica, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.SucceedFunction(
            storedId,
            result,
            timestamp,
            expectedReplica
        ).ToNpgsqlCommand(conn);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        bool ignoreInterrupted,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.PostponeFunction(
            storedId,
            postponeUntil,
            timestamp,
            ignoreInterrupted,
            expectedReplica
        ).ToNpgsqlCommand(conn);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        ReplicaId expectedReplica, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.FailFunction(
            storedId,
            storedException,
            timestamp,
            expectedReplica
        ).ToNpgsqlCommand(conn);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> SuspendFunction(
        StoredId storedId, 
        long timestamp,
        ReplicaId expectedReplica, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
    {
        await using var conn = await CreateConnection();

        await using var command = _sqlGenerator
            .SuspendFunction(storedId, timestamp, expectedReplica)
            .ToNpgsqlCommand(conn);
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getReplicasSql;
    public async Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas()
    {
        await using var conn = await CreateConnection();
        _getReplicasSql ??= @$"
            SELECT DISTINCT(Owner)
            FROM {_tableName}
            WHERE Status = {(int) Status.Executing} AND Owner IS NOT NULL";
        
        await using var command = new NpgsqlCommand(_getReplicasSql, conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        var replicas = new List<ReplicaId>();
        while (await reader.ReadAsync())
            replicas.Add(reader.GetGuid(0).ToReplicaId());
        
        return replicas;
    }

    private string? _rescheduleFunctionsSql;
    public async Task RescheduleCrashedFunctions(ReplicaId replicaId)
    {
        await using var conn = await CreateConnection();
       
        _rescheduleFunctionsSql ??= $@"
            UPDATE {_tableName}
            SET status = {(int) Status.Postponed},
                expires = 0,
                owner = NULL
            WHERE 
                owner = $1";
        await using var command = new NpgsqlCommand(_rescheduleFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = replicaId.AsGuid}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        StoredId storedId,
        byte[]? param, byte[]? result,
        ReplicaId? expectedReplica)
    {
        await using var conn = await CreateConnection();
        
        _setParametersSql ??= $@"
            UPDATE {_tableName}
            SET param_json = $1,             
                result_json = $2
            WHERE id = $3 ";

        var sql = expectedReplica == null
            ? _setParametersSql + " AND owner IS NULL;"
            : _setParametersSql + $" AND owner = '{expectedReplica.AsGuid:N}'";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = param ?? (object) DBNull.Value },
                new() { Value = result ?? (object) DBNull.Value },
                new() { Value = storedId.AsGuid },
            }
        };
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _interruptSql;
    public async Task<bool> Interrupt(StoredId storedId)
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
                WHERE id = $1";
        
        await using var command = new NpgsqlCommand(_interruptSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.AsGuid },
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
                WHERE id = $1";
        await using var command = new NpgsqlCommand(_interruptedSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.AsGuid },
            }
        };
        return (bool?) await command.ExecuteScalarAsync();
    }

    private string? _getFunctionStatusSql;
    public async Task<Status?> GetFunctionStatus(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _getFunctionStatusSql ??= $@"
            SELECT status
            FROM {_tableName}
            WHERE id = $1;";
        await using var command = new NpgsqlCommand(_getFunctionStatusSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.AsGuid }
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            return (Status)reader.GetInt32(0);
        }

        return null;
    }

    public async Task<IReadOnlyList<StatusAndId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, status, expires
            FROM {_tableName}
            WHERE Id IN ({storedIds.Select(id => $"'{id}'").StringJoin(", ")})";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);
        
        var toReturn = new List<StatusAndId>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var instance = reader.GetGuid(0);
            var status = (Status) reader.GetInt32(1);
            var expires = reader.GetInt64(2);

            var storedId = new StoredId(instance);
            toReturn.Add(new StatusAndId(storedId, status, expires));
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
                interrupted,
                timestamp,
                human_instance_id,
                parent,
                owner
            FROM {_tableName}
            WHERE id = $1;";
        await using var command = new NpgsqlCommand(_getFunctionSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.AsGuid}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(storedId, reader);
    }

    private async Task<StoredFlow?> ReadToStoredFunction(StoredId storedId, NpgsqlDataReader reader)
    {
        /*
           0  param_json,         
           1  status,
           2  result_json,         
           3  exception_json,
           4  expires,     
           5 interrupted,
           6 timestamp,
           7 human_instance_id
           8 parent
           9 owner
         */
        while (await reader.ReadAsync())
        {
            var hasParameter = !await reader.IsDBNullAsync(0);
            var hasResult = !await reader.IsDBNullAsync(2);
            var hasException = !await reader.IsDBNullAsync(3);
            var hasParent = !await reader.IsDBNullAsync(8);
            var hasOwner = !await reader.IsDBNullAsync(9);
            
            return new StoredFlow(
                storedId,
                HumanInstanceId: reader.GetString(7),
                hasParameter ? (byte[]) reader.GetValue(0) : null,
                Status: (Status) reader.GetInt32(1),
                Result: hasResult ? (byte[]) reader.GetValue(2) : null, 
                Exception: !hasException ? null : JsonSerializer.Deserialize<StoredException>(reader.GetString(3)),
                Expires: reader.GetInt64(4),
                Interrupted: reader.GetBoolean(5),
                Timestamp: reader.GetInt64(6),
                ParentId: hasParent ? StoredId.Deserialize(reader.GetString(8)) : null,
                OwnerId: hasOwner ? reader.GetGuid(9).ToReplicaId() : null,
                StoredType: storedId.Type
            );
        }

        return null;
    }
    
    public async Task<bool> DeleteFunction(StoredId storedId)
    {
        await _messageStore.Truncate(storedId);
        await _effectsStore.Remove(storedId);
        await _correlationStore.RemoveCorrelations(storedId);

        return await DeleteStoredFunction(storedId);
    }

    public IFunctionStore WithPrefix(string prefix)
        => new PostgreSqlFunctionStore(
            _connectionString,
            prefix
        );

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _deleteFunctionSql ??= @$"
            DELETE FROM {_tableName}
            WHERE id = $1;";

        await using var command = new NpgsqlCommand(_deleteFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.AsGuid},
            }
        };
       
        return await command.ExecuteNonQueryAsync() == 1;
    }
    
    private async Task<bool> DoTablesAlreadyExist()
    {
        await using var conn = await CreateConnection();
        
        var sql = $"SELECT 1 FROM {_tableName} LIMIT 1;";
        
        await using var command = new NpgsqlCommand(sql, conn);
        try
        {
            await command.ExecuteScalarAsync();
            return true;    
        } catch (NpgsqlException e)
        {
            const string undefinedTable = "42P01";
            if (e.SqlState == undefinedTable)
                return false;

            throw;
        }
    }
}