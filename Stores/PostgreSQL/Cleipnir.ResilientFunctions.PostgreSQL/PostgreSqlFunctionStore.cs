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
using Cleipnir.ResilientFunctions.Storage.Session;
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
        _effectsStore = new PostgreSqlEffectsStore(connectionString, _tableName);
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
                status INT NOT NULL DEFAULT {(int) Status.Executing},
                owner UUID NULL,
                timestamp BIGINT NOT NULL,
                param_json BYTEA NULL,
                result_json BYTEA NULL,
                exception_json TEXT NULL,
                human_instance_id TEXT NOT NULL,
                parent UUID NULL
            );

            CREATE INDEX IF NOT EXISTS idx_{_tableName}_expires
            ON {_tableName}(expires, id)
            WHERE status = {(int) Status.Postponed};";

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
    
    public async Task<IStorageSession?> CreateFunction(
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
            await using var batch = _sqlGenerator.CreateFunction(
                storedId,
                humanInstanceId,
                param,
                leaseExpiration,
                postponeUntil,
                timestamp,
                parent,
                owner,
                ignoreConflict: true
            ).CreateBatch().WithConnection(conn);

            var affectedRows = await batch.ExecuteNonQueryAsync();
            if (affectedRows != 1 || owner == null) 
                return null;
            
            return new SnapshotStorageSession(owner);
        }

        try
        {
            var commands = new List<StoreCommand>();
            commands.AddRange(_sqlGenerator.CreateFunction(
                storedId,
                humanInstanceId,
                param,
                leaseExpiration,
                postponeUntil,
                timestamp,
                parent,
                owner,
                ignoreConflict: false
            ));
            
            var session = new SnapshotStorageSession(owner ?? ReplicaId.Empty);
            if (effects?.Any() ?? false)
                commands.AddRange(
                    _sqlGenerator.InsertEffects(
                        storedId,
                        changes: effects.Select(e => new StoredEffectChange(storedId, e.EffectId, CrudOperation.Insert, e)).ToList(),
                        session
                    )
                );

            if (messages?.Any() ?? false)
                commands.AddRange(_sqlGenerator.AppendMessages(
                        messages.Select((msg, position) => new StoredIdAndMessageWithPosition(storedId, msg, position)).ToList()
                    )
                );

            await using var batch = commands.ToNpgsqlBatch();
            await using var conn = await CreateConnection();
            await using var transaction = await conn.BeginTransactionAsync();
            batch.WithConnection(conn, transaction);
            await batch.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
            
            return owner == null ? null : session;
        }
        catch (PostgresException e) when (e.SqlState == "23505")
        {
            return null;
        }
    }

    public async Task<int> BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
    {
        await using var conn = await CreateConnection();
        var chunks = functionsWithParam.Chunk(1000);
        var totalInserted = 0;
        foreach (var chunk in chunks)
        {
            var commands = new List<StoreCommand>();
            foreach (var idWithParam in chunk)
                commands.AddRange(_sqlGenerator.BulkScheduleFunctions(idWithParam, parent));

            await using var batch = commands.ToNpgsqlBatch().WithConnection(conn);
            var affectedRows = await batch.ExecuteNonQueryAsync();
            totalInserted += affectedRows;
        }
        return totalInserted;
    }

    public async Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, ReplicaId replicaId)
    {
        var restartCommand = _sqlGenerator.RestartExecution(storedId, replicaId);
        var effectsCommand = _sqlGenerator.GetEffects(storedId);
        var messagesCommand = _sqlGenerator.GetMessages(storedId, skip: 0);

        await using var conn = await CreateConnection();
        await using var command = StoreCommandExtensions
            .ToNpgsqlBatch([restartCommand, effectsCommand, messagesCommand])
            .WithConnection(conn);

        await using var reader = await command.ExecuteReaderAsync();
        if (reader.RecordsAffected == 0)
            return null;

        var sf = await _sqlGenerator.ReadFunction(storedId, reader);
        if (sf == null)
            return null;

        await reader.NextResultAsync();
        var (effects, session) = await _sqlGenerator.ReadEffects(reader, replicaId);

        await reader.NextResultAsync();
        var messages = await _sqlGenerator.ReadMessages(reader);
        var storedMessages = messages.Select(m => PostgreSqlMessageStore.ConvertToStoredMessage(m.content, m.position)).ToList();

        return new StoredFlowWithEffectsAndMessages(sf, effects, storedMessages, session);
    }

    public async Task<Dictionary<StoredId, StoredFlowWithEffectsAndMessages>> RestartExecutions(
        IReadOnlyList<StoredId> storedIds,
        ReplicaId owner)
    {
        if (storedIds.Count == 0)
            return new Dictionary<StoredId, StoredFlowWithEffectsAndMessages>();

        // Execute 3 queries in parallel
        var restartTask = RestartFlowsAsync(storedIds, owner);
        var effectsTask = FetchEffectsAsync(storedIds, owner);
        var messagesTask = FetchMessagesAsync(storedIds);

        await Task.WhenAll(restartTask, effectsTask, messagesTask);

        var restartedFlows = await restartTask;
        var effectsMap = await effectsTask;
        var messagesMap = await messagesTask;

        // Build result dictionary - only for successfully restarted flows
        var result = new Dictionary<StoredId, StoredFlowWithEffectsAndMessages>();
        foreach (var flow in restartedFlows)
        {
            var effects = effectsMap.TryGetValue(flow.StoredId, out var session)
                ? session.Effects.Values.ToList()
                : new List<StoredEffect>();
            var messages = messagesMap.TryGetValue(flow.StoredId, out var msgs)
                ? msgs
                : new List<StoredMessage>();
            var storageSession = effectsMap.TryGetValue(flow.StoredId, out var s)
                ? s
                : new SnapshotStorageSession(owner);

            result[flow.StoredId] = new StoredFlowWithEffectsAndMessages(
                flow, effects, messages, storageSession
            );
        }

        return result;
    }

    private async Task<List<StoredFlow>> RestartFlowsAsync(
        IReadOnlyList<StoredId> storedIds,
        ReplicaId owner)
    {
        await using var conn = await CreateConnection();
        var storeCommand = _sqlGenerator.RestartExecutions(storedIds, owner);

        await using var command = storeCommand.ToNpgsqlCommand(conn);
        await using var reader = await command.ExecuteReaderAsync();

        var flows = new List<StoredFlow>();
        var storedId = new StoredId(Guid.Empty);
        while (await reader.ReadAsync())
        {
            storedId = reader.GetGuid(0).ToStoredId();
            var hasParameter = !await reader.IsDBNullAsync(1);
            var hasResult = !await reader.IsDBNullAsync(3);
            var hasException = !await reader.IsDBNullAsync(4);
            var hasParent = !await reader.IsDBNullAsync(9);
            var hasOwner = !await reader.IsDBNullAsync(10);

            var param = hasParameter ? (byte[])reader.GetValue(1) : null;
            var status = (Status)reader.GetInt32(2);
            var result = hasResult ? (byte[])reader.GetValue(3) : null;
            var exception = hasException ? JsonSerializer.Deserialize<StoredException>(reader.GetString(4)) : null;
            var expires = reader.GetInt64(5);
            var interrupted = reader.GetBoolean(6);
            var timestamp = reader.GetInt64(7);
            var humanInstanceId = reader.GetString(8);
            var parent = hasParent ? reader.GetGuid(9).ToStoredId() : null;
            var ownerId = hasOwner ? new ReplicaId(reader.GetGuid(10)) : null;

            flows.Add(new StoredFlow(
                storedId,
                humanInstanceId,
                param,
                status,
                exception,
                expires,
                timestamp,
                interrupted,
                parent,
                ownerId,
                storedId.Type
            ));
        }
        return flows;
    }

    private async Task<Dictionary<StoredId, SnapshotStorageSession>> FetchEffectsAsync(
        IReadOnlyList<StoredId> storedIds,
        ReplicaId owner)
    {
        await using var conn = await CreateConnection();
        var storeCommand = _sqlGenerator.GetEffects(storedIds);

        await using var command = storeCommand.ToNpgsqlCommand(conn);
        await using var reader = await command.ExecuteReaderAsync();

        return await _sqlGenerator.ReadEffectsForIds(reader, storedIds, owner);
    }

    private async Task<Dictionary<StoredId, IReadOnlyList<StoredMessage>>> FetchMessagesAsync(IReadOnlyList<StoredId> storedIds)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.GetMessages(storedIds).ToNpgsqlCommand(conn);
        await using var reader = await command.ExecuteReaderAsync();
        var messages = await _sqlGenerator.ReadMessagesForMultipleStores(reader);
        return messages;
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

    public async Task<IReadOnlyList<StoredId>> GetInterruptedFunctions(IEnumerable<StoredId> ids)
    {
        var idsArray = ids.Select(id => id.AsGuid).ToArray();
        if (idsArray.Length == 0)
            return [];

        var sql = @$"
            SELECT id
            FROM {_tableName}
            WHERE interrupted = TRUE AND id = ANY($1)";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);
        command.Parameters.Add(new NpgsqlParameter { Value = idsArray });

        await using var reader = await command.ExecuteReaderAsync();
        var interruptedIds = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var storedId = reader.GetGuid(0).ToStoredId();
            interruptedIds.Add(storedId);
        }

        return interruptedIds;
    }

    private string? _setFunctionStateSqlMain;
    private string? _setFunctionStateSqlMainWithoutReplica;

    public async Task<bool> SetFunctionState(
        StoredId storedId, Status status,
        byte[]? param, byte[]? result,
        StoredException? storedException,
        long expires,
        ReplicaId? expectedReplica)
    {
        await using var conn = await CreateConnection();

        if (expectedReplica == null)
        {
            _setFunctionStateSqlMainWithoutReplica ??= $@"
                UPDATE {_tableName}
                SET status = $1, expires = $2, param_json = $3, result_json = $4, exception_json = $5
                WHERE id = $6 AND owner IS NULL";

            await using var command = new NpgsqlCommand(_setFunctionStateSqlMainWithoutReplica, conn)
            {
                Parameters =
                {
                    new() {Value = (int) status},
                    new() {Value = expires },
                    new() {Value = param == null ? DBNull.Value : param},
                    new() {Value = result == null ? DBNull.Value : result},
                    new() {Value = storedException == null ? DBNull.Value : JsonSerializer.Serialize(storedException)},
                    new() {Value = storedId.AsGuid},
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows == 1;
        }
        else
        {
            _setFunctionStateSqlMain ??= $@"
                UPDATE {_tableName}
                SET status = $1, expires = $2, param_json = $3, result_json = $4, exception_json = $5
                WHERE id = $6 AND owner = $7";

            await using var command = new NpgsqlCommand(_setFunctionStateSqlMain, conn)
            {
                Parameters =
                {
                    new() {Value = (int) status},
                    new() {Value = expires },
                    new() {Value = param == null ? DBNull.Value : param},
                    new() {Value = result == null ? DBNull.Value : result},
                    new() {Value = storedException == null ? DBNull.Value : JsonSerializer.Serialize(storedException)},
                    new() {Value = storedId.AsGuid},
                    new() {Value = expectedReplica.AsGuid},
                }
            };

            var affectedRows = await command.ExecuteNonQueryAsync();
            return affectedRows == 1;
        }
    }

    public async Task<bool> SucceedFunction(
        StoredId storedId,
        byte[]? result,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        await using var conn = await CreateConnection();
        await using var batch = _sqlGenerator.SucceedFunction(
            storedId,
            result,
            timestamp,
            expectedReplica
        ).CreateBatch().WithConnection(conn);

        var affectedRows = await batch.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.PostponeFunction(
            storedId,
            postponeUntil,
            timestamp,
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
        IStorageSession? storageSession)
    {
        await using var conn = await CreateConnection();
        await using var batch = _sqlGenerator.FailFunction(
            storedId,
            storedException,
            timestamp,
            expectedReplica
        ).CreateBatch().WithConnection(conn);

        var affectedRows = await batch.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }
    
    public async Task<bool> SuspendFunction(
        StoredId storedId,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession)
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

    public async Task<bool> SetParameters(
        StoredId storedId,
        byte[]? param, byte[]? result,
        ReplicaId? expectedReplica)
    {
        await using var conn = await CreateConnection();
        var storeCommand = _sqlGenerator.SetParameters(storedId, param, result, expectedReplica);
        await using var command = storeCommand.ToNpgsqlCommand(conn);

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
        var ids = storedIds.Select(id => id.AsGuid).ToArray();
        if (!ids.Any())
            return [];
            
        var sql = @$"
            SELECT id, status, expires
            FROM {_tableName}
            WHERE Id = ANY($1)";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);
        command.Parameters.Add(new NpgsqlParameter { Value = ids });

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
                InstanceId: reader.GetString(7),
                hasParameter ? (byte[]) reader.GetValue(0) : null,
                Status: (Status) reader.GetInt32(1),
                Exception: !hasException ? null : JsonSerializer.Deserialize<StoredException>(reader.GetString(3)),
                Expires: reader.GetInt64(4),
                Timestamp: reader.GetInt64(6),
                Interrupted: reader.GetBoolean(5),
                ParentId: hasParent ? reader.GetGuid(8).ToStoredId() : null,
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

    public async Task<IReadOnlyDictionary<StoredId, byte[]?>> GetResults(IEnumerable<StoredId> storedIds)
    {
        var idsArray = storedIds.Select(id => id.AsGuid).ToArray();
        if (idsArray.Length == 0)
            return new Dictionary<StoredId, byte[]?>();

        var sql = @$"
            SELECT id, result_json
            FROM {_tableName}
            WHERE id = ANY($1)";

        await using var conn = await CreateConnection();
        await using var command = new NpgsqlCommand(sql, conn);
        command.Parameters.Add(new NpgsqlParameter { Value = idsArray });

        await using var reader = await command.ExecuteReaderAsync();
        var results = new Dictionary<StoredId, byte[]?>();
        while (await reader.ReadAsync())
        {
            var storedId = new StoredId(reader.GetGuid(0));
            var hasResult = !await reader.IsDBNullAsync(1);
            var result = hasResult ? (byte[])reader.GetValue(1) : null;
            results[storedId] = result;
        }

        return results;
    }

    private string? _setResultSql;
    public async Task SetResult(StoredId storedId, byte[] result, ReplicaId expectedReplica)
    {
        await using var conn = await CreateConnection();
        _setResultSql ??= $@"
            UPDATE {_tableName}
            SET result_json = $1
            WHERE id = $2 AND owner = $3";

        await using var command = new NpgsqlCommand(_setResultSql, conn)
        {
            Parameters =
            {
                new() { Value = result },
                new() { Value = storedId.AsGuid },
                new() { Value = expectedReplica.AsGuid }
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _deleteFunctionSql ??= $"DELETE FROM {_tableName} WHERE id = $1";

        await using var command = new NpgsqlCommand(_deleteFunctionSql, conn)
        {
            Parameters = { new() { Value = storedId.AsGuid } }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows >= 1;
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