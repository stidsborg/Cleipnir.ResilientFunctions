using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Storage.Utils;
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
    
    private readonly PostgresCommandExecutor _commandExecutor;

    private readonly PostgreSqlDbReplicaStore _replicaStore;
    public IReplicaStore ReplicaStore => _replicaStore;

    private readonly SqlGenerator _sqlGenerator;

    public PostgreSqlFunctionStore(string connectionString, string tablePrefix = "")
    {
        _tableName = tablePrefix == "" ? "rfunctions" : tablePrefix;
        _connectionString = connectionString;
        _sqlGenerator = new SqlGenerator(_tableName);

        _messageStore = new PostgreSqlMessageStore(connectionString, _sqlGenerator, _tableName);
        _commandExecutor = new PostgresCommandExecutor(connectionString);
        _typeStore = new PostgreSqlTypeStore(connectionString, _tableName);
        _replicaStore = new PostgreSqlDbReplicaStore(connectionString, _tableName);
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

        await _messageStore.Initialize();
        await _typeStore.Initialize();
        await _replicaStore.Initialize();
        await using var conn = await CreateConnection();
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tableName} (
                id UUID PRIMARY KEY,
                expires BIGINT NOT NULL,
                status INT NOT NULL DEFAULT {(int) Status.Executing},
                owner UUID NULL,
                timestamp BIGINT NOT NULL,
                param BYTEA NULL,
                result BYTEA NULL,
                exception TEXT NULL,
                human_instance_id TEXT NOT NULL,
                parent UUID NULL,
                effects BYTEA NULL,
                version INT NOT NULL DEFAULT 0
            ) WITH (fillfactor = 80);

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
        await _typeStore.Truncate();
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
        long? postponeUntil,
        long timestamp,
        StoredId? parent,
        ReplicaId? owner,
        IReadOnlyList<StoredEffect>? effects = null
        )
    {
        if (effects == null)
        {
            await using var conn = await CreateConnection();
            await using var batch = _sqlGenerator.CreateFunction(
                storedId,
                humanInstanceId,
                param,
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
                postponeUntil,
                timestamp,
                parent,
                owner,
                ignoreConflict: false
            ));
            
            var session = new SnapshotStorageSession(owner);
            if (effects?.Any() ?? false)
                commands.AddRange(
                    _sqlGenerator.InsertEffects(
                        storedId,
                        changes: effects.Select(e => new StoredEffectChange(storedId, e.EffectId, CrudOperation.Insert, e)).ToList(),
                        session
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

    private static (List<StoredEffect> Effects, SnapshotStorageSession Session) ReadEffectsColumn(byte[]? effectsBytes, ReplicaId owner)
    {
        var effects = new List<StoredEffect>();
        var session = new SnapshotStorageSession(owner);

        if (effectsBytes == null)
            return (effects, session);

        foreach (var effectBytes in BinaryPacker.Split(effectsBytes))
        {
            var storedEffect = StoredEffect.Deserialize(effectBytes!);
            effects.Add(storedEffect);
            session.Effects[storedEffect.EffectId] = storedEffect;
        }

        return (effects, session);
    }

    public async Task<Dictionary<StoredId, StoredFlowWithEffects>> RestartExecutions(
        IReadOnlyList<StoredId> storedIds,
        ReplicaId owner)
    {
        if (storedIds.Count == 0)
            return new Dictionary<StoredId, StoredFlowWithEffects>();

        // The claim UPDATE returns each restarted flow's effects column - claim and effect snapshot are one
        // atomic statement per flow.
        await using var conn = await CreateConnection();
        var storeCommand = _sqlGenerator.RestartExecutions(storedIds, owner);

        await using var command = storeCommand.ToNpgsqlCommand(conn);
        await using var reader = await command.ExecuteReaderAsync();

        var result = new Dictionary<StoredId, StoredFlowWithEffects>();
        while (await reader.ReadAsync())
        {
            var storedId = reader.GetGuid(0).ToStoredId();
            var hasParameter = !await reader.IsDBNullAsync(1);
            var hasException = !await reader.IsDBNullAsync(4);
            var hasParent = !await reader.IsDBNullAsync(8);
            var hasOwner = !await reader.IsDBNullAsync(9);
            var hasEffects = !await reader.IsDBNullAsync(10);

            var param = hasParameter ? (byte[])reader.GetValue(1) : null;
            var status = (Status)reader.GetInt32(2);
            var exception = hasException ? JsonSerializer.Deserialize<StoredException>(reader.GetString(4)) : null;
            var expires = reader.GetInt64(5);
            var timestamp = reader.GetInt64(6);
            var humanInstanceId = reader.GetString(7);
            var parent = hasParent ? reader.GetGuid(8).ToStoredId() : null;
            var ownerId = hasOwner ? new ReplicaId(reader.GetGuid(9)) : null;

            var flow = new StoredFlow(
                storedId,
                humanInstanceId,
                param,
                status,
                exception,
                expires,
                timestamp,
                parent,
                ownerId,
                storedId.Type
            );

            var (effects, session) = ReadEffectsColumn(
                hasEffects ? (byte[])reader.GetValue(10) : null,
                owner
            );

            result[storedId] = new StoredFlowWithEffects(flow, effects, session);
        }

        return result;
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
                SET status = $1, expires = $2, param = $3, result = $4, exception = $5
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
                SET status = $1, expires = $2, param = $3, result = $4, exception = $5
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

    public async Task<bool> SetStatus(
        StoredId storedId,
        Status status,
        byte[]? result,
        StoredException? storedException,
        long expires,
        long timestamp,
        ReplicaId expectedReplica,
        IStorageSession? storageSession)
    {
        await using var conn = await CreateConnection();
        await using var command = _sqlGenerator.SetStatus(
            storedId,
            status,
            result,
            storedException,
            expires,
            timestamp,
            expectedReplica
        ).ToNpgsqlCommand(conn);

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
                param,
                status,
                result,
                exception,
                expires,
                timestamp,
                human_instance_id,
                parent,
                owner,
                effects,
                version
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
           0  param,
           1  status,
           2  result,
           3  exception,
           4  expires,
           5 timestamp,
           6 human_instance_id
           7 parent
           8 owner
           9 effects
           10 version
         */
        while (await reader.ReadAsync())
        {
            var hasParameter = !await reader.IsDBNullAsync(0);
            var hasResult = !await reader.IsDBNullAsync(2);
            var hasException = !await reader.IsDBNullAsync(3);
            var hasParent = !await reader.IsDBNullAsync(7);
            var hasOwner = !await reader.IsDBNullAsync(8);
            var hasEffects = !await reader.IsDBNullAsync(9);

            return new StoredFlow(
                storedId,
                InstanceId: reader.GetString(6),
                hasParameter ? (byte[]) reader.GetValue(0) : null,
                Status: (Status) reader.GetInt32(1),
                Exception: !hasException ? null : JsonSerializer.Deserialize<StoredException>(reader.GetString(3)),
                Expires: reader.GetInt64(4),
                Timestamp: reader.GetInt64(5),
                ParentId: hasParent ? reader.GetGuid(7).ToStoredId() : null,
                OwnerId: hasOwner ? reader.GetGuid(8).ToReplicaId() : null,
                StoredType: storedId.Type,
                Effects: DeserializeEffects(hasEffects ? (byte[]) reader.GetValue(9) : null),
                Version: reader.GetInt32(10)
            );
        }

        return null;
    }

    private static IReadOnlyList<StoredEffect> DeserializeEffects(byte[]? effectsBytes)
    {
        if (effectsBytes == null)
            return [];

        var effects = new List<StoredEffect>();
        foreach (var effectBytes in BinaryPacker.Split(effectsBytes))
            effects.Add(StoredEffect.Deserialize(effectBytes!));

        return effects;
    }
    
    public async Task<bool> DeleteFunction(StoredId storedId)
    {
        await _messageStore.Truncate(storedId);

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
            SELECT id, result
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

    // Effects live in the 'effects' column on the flows row. Owned writes are guarded by the owner column alone;
    // unowned writes (null-owner session or no session) are additionally guarded by the version column, which is
    // bumped by every claim and every unowned write - see IFunctionStore.SetEffectResults.
    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
    {
        if (changes.Count == 0)
            return;

        var owner = default(ReplicaId);
        var version = 0;
        var existingEffects = new Dictionary<EffectId, StoredEffect>();
        var snapshotSession = session as SnapshotStorageSession;
        if (snapshotSession != null)
        {
            existingEffects = snapshotSession.Effects;
            owner = snapshotSession.ReplicaId;
            version = snapshotSession.Version;
        }
        else
        {
            // Single read gets the existing effects plus the owner and version to guard the write on.
            var storedFlow = await GetFunction(storedId);
            foreach (var e in storedFlow?.Effects ?? [])
                existingEffects[e.EffectId] = e;
            owner = storedFlow?.OwnerId;
            version = storedFlow?.Version ?? 0;
        }

        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                existingEffects.Remove(change.EffectId);
            else
                existingEffects[change.EffectId] = change.StoredEffect!;

        var content = SnapshotStorageSession.Serialize(existingEffects);

        var command =
            owner != null
                ? StoreCommand.Create(
                    $"UPDATE {_tableName} SET effects = $1 WHERE id = $2 AND owner = $3",
                    [content, storedId.AsGuid, owner.AsGuid]
                )
                : StoreCommand.Create(
                    $"UPDATE {_tableName} SET effects = $1, version = version + 1 WHERE id = $2 AND owner IS NULL AND version = $3",
                    [content, storedId.AsGuid, version]
                );

        var affectedRows = await _commandExecutor.ExecuteNonQuery(command);
        if (affectedRows == 0)
            throw UnexpectedStateException.ConcurrentModification(storedId);

        if (snapshotSession is { ReplicaId: null })
            snapshotSession.Version++;
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