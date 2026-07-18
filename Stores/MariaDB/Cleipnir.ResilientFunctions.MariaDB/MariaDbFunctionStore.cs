using System.Text;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.MariaDB.StoreCommand;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Storage.Utils;
using MySqlConnector;
using static Cleipnir.ResilientFunctions.MariaDb.DatabaseHelper;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbFunctionStore : IFunctionStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    private readonly MariaDbMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;
    
    private readonly MariaDbCommandExecutor _commandExecutor;

    private readonly MariaDbTypeStore _typeStore;
    public ITypeStore TypeStore => _typeStore;

    private readonly MariaDbReplicaStore _replicaStore;
    public IReplicaStore ReplicaStore => _replicaStore;

    private readonly SqlGenerator _sqlGenerator;

    public MariaDbFunctionStore(string connectionString, string tablePrefix = "")
    {
        tablePrefix = tablePrefix == "" ? "rfunctions" : tablePrefix;

        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
        _sqlGenerator = new SqlGenerator(tablePrefix);

        _messageStore = new MariaDbMessageStore(connectionString, _sqlGenerator, tablePrefix);
        _commandExecutor = new MariaDbCommandExecutor(connectionString);
        _typeStore = new MariaDbTypeStore(connectionString, tablePrefix);
        _replicaStore = new MariaDbReplicaStore(connectionString, tablePrefix);
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        if (await DoTablesAlreadyExist())
            return;

        await MessageStore.Initialize();
        await _typeStore.Initialize();
        await _replicaStore.Initialize();
        await using var conn = await CreateOpenConnection(_connectionString);
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {_tablePrefix} (
                id CHAR(32) PRIMARY KEY,
                status INT NOT NULL,
                expires BIGINT NOT NULL,
                param LONGBLOB NULL,
                result LONGBLOB NULL,
                exception TEXT NULL,
                timestamp BIGINT NOT NULL,
                human_instance_id TEXT NOT NULL,
                parent CHAR(32) NULL,
                owner CHAR(32) NULL,
                effects LONGBLOB NULL,
                version INT NOT NULL DEFAULT 0,
                INDEX (expires, id, status)
            );";

        await using var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTablesSql;
    public async Task TruncateTables()
    {
        await _messageStore.TruncateTable();
        await _typeStore.Truncate();
        await _replicaStore.Truncate();
        
        await using var conn = await CreateOpenConnection(_connectionString);
        _truncateTablesSql ??= $"TRUNCATE TABLE {_tablePrefix}";
        await using var command = new MySqlCommand(_truncateTablesSql, conn);
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
        IReadOnlyList<StoredEffect>? effects = null)
    {
        var session = new SnapshotStorageSession();

        // Serialize effects if present
        byte[]? effectsBytes = null;
        if (effects?.Any() ?? false)
        {
            foreach (var effect in effects)
                session.Effects[effect.EffectId] = effect;
            effectsBytes = session.Serialize();
        }

        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .CreateFunction(storedId, humanInstanceId, param, postponeUntil, timestamp, parent, owner, ignoreDuplicate: true, effects: effectsBytes)
            .ToSqlCommand(conn);
        var affectedRows = await command.ExecuteNonQueryAsync();
        if (affectedRows != 1 || owner == null)
            return null;

        return session;
    }

    public async Task<int> BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
    {
        var insertSql = @$"
            INSERT IGNORE INTO {_tablePrefix}
              (id, param, status, expires, timestamp, human_instance_id, parent, owner)
            VALUES
                    ";

        var now = DateTime.UtcNow.Ticks;
        var parentStr = parent == null ? "NULL" : $"'{parent.AsGuid:N}'";

        var chunks = functionsWithParam.Chunk(500);
        var totalInserted = 0;
        foreach (var chunk in chunks)
        {
            var rows = new List<string>();
            foreach (var (storedId, humanInstanceId, param) in chunk)
            {
                var id = storedId.AsGuid;
                var row = $"('{id:N}', {(param == null ? "NULL" : $"x'{Convert.ToHexString(param)}'")}, {(int) Status.Postponed}, 0, {now}, '{humanInstanceId.EscapeString()}', {parentStr}, NULL)";
                rows.Add(row);
            }
            var rowsSql = string.Join(", " + Environment.NewLine, rows);
            var strBuilder = new StringBuilder(rowsSql.Length + 2);
            strBuilder.Append(insertSql);
            strBuilder.Append(rowsSql);
            strBuilder.Append(";");
            var sql = strBuilder.ToString();

            await using var conn = await CreateOpenConnection(_connectionString);
            await using var cmd = new MySqlCommand(sql, conn);
            var affectedRows = await cmd.ExecuteNonQueryAsync();
            totalInserted += affectedRows;
        }
        return totalInserted;
    }
    
    public async Task<Dictionary<StoredId, StoredFlowWithEffects>> RestartExecutions(
        IReadOnlyList<StoredId> storedIds,
        ReplicaId owner)
    {
        if (storedIds.Count == 0)
            return new Dictionary<StoredId, StoredFlowWithEffects>();

        // Execute restart (effects are returned inline)
        var restartedFlows = await RestartFlowsAsync(storedIds, owner);

        // Build result dictionary - only for successfully restarted flows
        var result = new Dictionary<StoredId, StoredFlowWithEffects>();
        foreach (var (flow, effectsBytes, session) in restartedFlows)
        {
            var effects = new List<StoredEffect>();
            if (effectsBytes != null)
            {
                var effectsBytesArray = BinaryPacker.Split(effectsBytes);
                foreach (var effectBytes in effectsBytesArray)
                {
                    if (effectBytes != null)
                    {
                        var storedEffect = StoredEffect.Deserialize(effectBytes);
                        effects.Add(storedEffect);
                        session.Effects[storedEffect.EffectId] = storedEffect;
                    }
                }
            }

            result[flow.StoredId] = new StoredFlowWithEffects(
                flow, effects, session
            );
        }

        return result;
    }

    private async Task<List<(StoredFlow flow, byte[]? effectsBytes, SnapshotStorageSession session)>> RestartFlowsAsync(
        IReadOnlyList<StoredId> storedIds,
        ReplicaId owner)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        // The locking SELECT and the claiming UPDATE run in one transaction so the selected rows are exactly the
        // rows this call claims - a plain UPDATE-then-SELECT returns rows already claimed by an earlier call from
        // the same replica as if they were claimed now, causing two concurrent claimers to restart the same flow.
        await using var transaction = await conn.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted);

        const int idIndex = 0;
        const int paramIndex = 1;
        const int resultIndex = 3;
        const int exceptionIndex = 4;
        const int timestampIndex = 6;
        const int humanInstanceIdIndex = 7;
        const int parentIndex = 8;
        const int effectsIndex = 10;

        var flows = new List<(StoredFlow flow, byte[]? effectsBytes, SnapshotStorageSession session)>();
        await using (var selectCommand = _sqlGenerator.RestartExecutionsSelectEligible(storedIds).ToSqlCommand(conn))
        {
            selectCommand.Transaction = transaction;
            await using var reader = await selectCommand.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var id = reader.GetString(idIndex).ToGuid().ToStoredId();
                var hasParam = !await reader.IsDBNullAsync(paramIndex);
                var hasResult = !await reader.IsDBNullAsync(resultIndex);
                var hasError = !await reader.IsDBNullAsync(exceptionIndex);
                var hasParent = !await reader.IsDBNullAsync(parentIndex);
                var hasEffects = !await reader.IsDBNullAsync(effectsIndex);

                var param = hasParam ? (byte[])reader.GetValue(paramIndex) : null;
                var result = hasResult ? (byte[])reader.GetValue(resultIndex) : null;
                var exceptionJson = hasError ? reader.GetString(exceptionIndex) : null;
                var exception = exceptionJson == null ? null : StoredException.Deserialize(exceptionJson);
                var timestamp = reader.GetInt64(timestampIndex);
                var humanInstanceId = reader.GetString(humanInstanceIdIndex);
                var parent = hasParent ? reader.GetString(parentIndex).ToGuid().ToStoredId() : null;
                var effectsBytes = hasEffects ? (byte[])reader.GetValue(effectsIndex) : null;

                // The row is read before the claiming UPDATE runs, so status/expires/owner reflect the
                // pre-claim state - report the claimed values the UPDATE is about to write instead.
                var flow = new StoredFlow(
                    id,
                    humanInstanceId,
                    param,
                    Status.Executing,
                    exception,
                    Expires: 0,
                    timestamp,
                    parent,
                    OwnerId: owner,
                    id.Type
                );

                var session = new SnapshotStorageSession();
                flows.Add((flow, effectsBytes, session));
            }
        }

        if (flows.Count > 0)
        {
            var claimedIds = flows.Select(f => f.flow.StoredId).ToList();
            await using var claimCommand = _sqlGenerator.RestartExecutionsClaim(claimedIds, owner).ToSqlCommand(conn);
            claimCommand.Transaction = transaction;
            await claimCommand.ExecuteNonQueryAsync();
        }

        await transaction.CommitAsync();

        return flows;
    }

    private string? _getExpiredFunctionsSql;
    public async Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long expiredBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getExpiredFunctionsSql ??= @$"
            SELECT id
            FROM {_tablePrefix}
            WHERE expires <= ? AND status = {(int) Status.Postponed}";
        await using var command = new MySqlCommand(_getExpiredFunctionsSql, conn)
        {
            Parameters =
            {
                new() {Value = expiredBefore}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var guid = reader.GetString(0).ToGuid();
            var id = new StoredId(guid);
            ids.Add(id);
        }
        
        return ids;
    }

    private string? _getSucceededFunctionsSql;
    public async Task<IReadOnlyList<StoredId>> GetSucceededFunctions(long completedBefore)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getSucceededFunctionsSql ??= @$"
            SELECT id
            FROM {_tablePrefix}
            WHERE status = {(int) Status.Succeeded} AND timestamp <= ?";
        await using var command = new MySqlCommand(_getSucceededFunctionsSql, conn)
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
            var instance = reader.GetString(0).ToGuid().ToStoredId();
            ids.Add(instance);
        }
        
        return ids;
    }

    private string? _setFunctionStateSql;
    public async Task<bool> SetFunctionState(
        StoredId storedId, Status status, 
        byte[]? storedParameter, byte[]? storedResult, 
        StoredException? storedException, 
        long expires,
        ReplicaId? expectedReplica)
    {
        await using var conn = await CreateOpenConnection(_connectionString);

        _setFunctionStateSql ??= $@"
            UPDATE {_tablePrefix}
            SET status = ?, 
                param = ?,  
                result = ?,  
                exception = ?, expires = ?
            WHERE id = ?";
        
        var sql = expectedReplica == null
             ? _setFunctionStateSql + " AND owner IS NULL" 
             :  _setFunctionStateSql + $" AND owner = {expectedReplica}";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = (int) status},
                new() {Value = storedParameter ?? (object) DBNull.Value},
                new() {Value = storedResult ?? (object) DBNull.Value},
                new() {Value = storedException != null ? JsonSerializer.Serialize(storedException) : DBNull.Value},
                new() {Value = expires},
                new() {Value = storedId.AsGuid.ToString("N")},
            }
        };

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
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
        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = _sqlGenerator
            .SetStatus(storedId, status, result, storedException, expires, timestamp, expectedReplica)
            .ToSqlCommand(conn);

        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _getReplicasSql;
    public async Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas()
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getReplicasSql ??= @$"
            SELECT DISTINCT(Owner)
            FROM {_tablePrefix}
            WHERE Status = {(int) Status.Executing} AND Owner IS NOT NULL";
        
        await using var command = new MySqlCommand(_getReplicasSql, conn);
        
        await using var reader = await command.ExecuteReaderAsync();
        var replicas = new List<ReplicaId>();
        while (await reader.ReadAsync())
            replicas.Add(reader.GetString(0).ToGuid().ToReplicaId());
        
        return replicas;
    }

    private string? _rescheduleFunctionsSql;
    public async Task RescheduleCrashedFunctions(ReplicaId replicaId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _rescheduleFunctionsSql ??= $@"
            UPDATE {_tablePrefix}
            SET 
                status = {(int) Status.Postponed},
                expires = 0,
                owner = NULL
            WHERE 
                owner = ?";
        
        await using var command = new MySqlCommand(_rescheduleFunctionsSql, conn)
        {
            Parameters =
            {
                new() { Value = replicaId.AsGuid.ToString("N") },
            }
        };
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _setParametersSql;
    public async Task<bool> SetParameters(
        StoredId storedId,
        byte[]? storedParameter, byte[]? storedResult,
        ReplicaId? expectedReplica)
    {
        await using var conn = await CreateOpenConnection(_connectionString);

        _setParametersSql ??= $@"
            UPDATE {_tablePrefix}
            SET param = ?,  
                result = ?
            WHERE 
                id = ?";

        var sql = expectedReplica == null
            ? _setParametersSql + " AND owner IS NULL"
            : _setParametersSql + $" AND owner = '{expectedReplica.AsGuid:N}'";

        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = storedParameter ?? (object) DBNull.Value },
                new() { Value = storedResult ?? (object) DBNull.Value },
                new() { Value = storedId.AsGuid.ToString("N") }
            }
        };
            
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }


    private string? _getFunctionStatusSql;
    public async Task<Status?> GetFunctionStatus(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getFunctionStatusSql ??= $@"
            SELECT status
            FROM {_tablePrefix}
            WHERE id = ?;";
        await using var command = new MySqlCommand(_getFunctionStatusSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.AsGuid.ToString("N")}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        
        while (await reader.ReadAsync())
        {
            return (Status) reader.GetInt32(0);
        }

        return null;
    }

    public async Task<IReadOnlyList<StatusAndId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, status, expires
            FROM {_tablePrefix}
            WHERE Id in ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")})";

        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);
        
        var toReturn = new List<StatusAndId>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var guid = reader.GetGuid(0);
            var status = (Status) reader.GetInt32(1);
            var expires = reader.GetInt64(2);

            var storedId = new StoredId(guid);
            toReturn.Add(new StatusAndId(storedId, status, expires));
        }

        return toReturn;
    }

    private string? _getFunctionSql;
    public async Task<StoredFlow?> GetFunction(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
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
            FROM {_tablePrefix}
            WHERE id = ?;";
        await using var command = new MySqlCommand(_getFunctionSql, conn)
        {
            Parameters = { 
                new() {Value = storedId.AsGuid.ToString("N")}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        return await ReadToStoredFunction(storedId, reader);
    }

    private string? _getInstancesWithStatusSql;
    public async Task<IReadOnlyList<StoredId>> GetInstances(StoredType storedType, Status status)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getInstancesWithStatusSql ??= @$"
            SELECT instance
            FROM {_tablePrefix}
            WHERE type = ? AND status = ?";
        await using var command = new MySqlCommand(_getInstancesWithStatusSql, conn)
        {
            Parameters =
            {
                new() {Value = storedType.Value},
                new() {Value = (int) status}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var flowInstance = reader.GetString(0).ToGuid().ToStoredId();
            ids.Add(flowInstance);
        }
        
        return ids;
    }

    private string? _getInstancesSql;
    public async Task<IReadOnlyList<StoredId>> GetInstances(StoredType storedType)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        _getInstancesSql ??= @$"
            SELECT instance
            FROM {_tablePrefix}
            WHERE type = ?";
        await using var command = new MySqlCommand(_getInstancesSql, conn)
        {
            Parameters =
            {
                new() {Value = storedType.Value}
            }
        };
        
        await using var reader = await command.ExecuteReaderAsync();
        var ids = new List<StoredId>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0).ToGuid().ToStoredId();
            ids.Add(id);
        }
        
        return ids;
    }
    
    private async Task<StoredFlow?> ReadToStoredFunction(StoredId storedId, MySqlDataReader reader)
    {
        const int paramIndex = 0;
        const int statusIndex = 1;
        const int resultIndex = 2;
        const int exceptionIndex = 3;
        const int expiresIndex = 4;
        const int timestampIndex = 5;
        const int humanInstanceIdIndex = 6;
        const int parentIndex = 7;
        const int ownerIndex = 8;
        const int effectsIndex = 9;
        const int versionIndex = 10;

        while (await reader.ReadAsync())
        {
            var hasParam = !await reader.IsDBNullAsync(paramIndex);
            var hasResult = !await reader.IsDBNullAsync(resultIndex);
            var hasError = !await reader.IsDBNullAsync(exceptionIndex);
            var hasParent = !await reader.IsDBNullAsync(parentIndex);
            var hasOwner = !await reader.IsDBNullAsync(ownerIndex);
            var hasEffects = !await reader.IsDBNullAsync(effectsIndex);
            var storedException = hasError
                ? JsonSerializer.Deserialize<StoredException>(reader.GetString(exceptionIndex))
                : null;
            return new StoredFlow(
                storedId,
                InstanceId: reader.GetString(humanInstanceIdIndex),
                hasParam ? (byte[]) reader.GetValue(paramIndex) : null,
                Status: (Status) reader.GetInt32(statusIndex),
                storedException,
                Expires: reader.GetInt64(expiresIndex),
                Timestamp: reader.GetInt64(timestampIndex),
                ParentId: hasParent ? reader.GetString(parentIndex).ToGuid().ToStoredId() : null,
                OwnerId: hasOwner ? reader.GetString(ownerIndex).ParseToReplicaId() : null,
                StoredType: storedId.Type,
                Effects: DeserializeEffects(hasEffects ? (byte[]) reader.GetValue(effectsIndex) : null),
                Version: reader.GetInt32(versionIndex)
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
    
    // Effects live in the 'effects' column on the flows row. Owned writes are guarded by the owner column alone;
    // unowned writes (null-owner session or no session) are additionally guarded by the version column, which is
    // bumped by every claim and every unowned write - see IFunctionStore.SetEffectResults.
    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, ReplicaId? owner, IStorageSession? session)
    {
        if (changes.Count == 0)
            return;

        var version = 0;
        var existingEffects = new Dictionary<EffectId, StoredEffect>();
        var snapshotSession = session as SnapshotStorageSession;
        if (snapshotSession != null)
        {
            existingEffects = snapshotSession.Effects;
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
                    $"UPDATE {_tablePrefix} SET effects = ? WHERE id = ? AND owner = ?",
                    [
                        content,
                        storedId.AsGuid.ToString("N"),
                        owner.AsGuid.ToString("N")
                    ])
                : StoreCommand.Create(
                    $@"UPDATE {_tablePrefix} SET effects = ?, version = version + 1 WHERE id = ? AND owner IS NULL AND version = ?",
                    [
                        content,
                        storedId.AsGuid.ToString("N"),
                        version
                    ]
                );

        var affectedRows = await _commandExecutor.ExecuteNonQuery(command);
        if (affectedRows == 0)
            throw UnexpectedStateException.ConcurrentModification(storedId);

        if (snapshotSession != null && owner == null)
            snapshotSession.Version++;
    }

    public async Task<bool> DeleteFunction(StoredId storedId)
    {
        await _messageStore.Truncate(storedId);

        return await DeleteStoredFunction(storedId);
    }

    public IFunctionStore WithPrefix(string prefix)
        => new MariaDbFunctionStore(_connectionString, prefix);

    public async Task<IReadOnlyDictionary<StoredId, byte[]?>> GetResults(IEnumerable<StoredId> storedIds)
    {
        var inSql = storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ");
        if (inSql == "")
            return new Dictionary<StoredId, byte[]?>();

        var sql = @$"
            SELECT id, result
            FROM {_tablePrefix}
            WHERE id IN ({inSql})";

        await using var conn = await CreateOpenConnection(_connectionString);
        await using var command = new MySqlCommand(sql, conn);

        await using var reader = await command.ExecuteReaderAsync();
        var results = new Dictionary<StoredId, byte[]?>();
        while (await reader.ReadAsync())
        {
            var guid = reader.GetString(0).ToGuid();
            var storedId = new StoredId(guid);
            var hasResult = !await reader.IsDBNullAsync(1);
            var result = hasResult ? (byte[])reader.GetValue(1) : null;
            results[storedId] = result;
        }

        return results;
    }

    private string? _deleteFunctionSql;
    private async Task<bool> DeleteStoredFunction(StoredId storedId)
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        _deleteFunctionSql ??= $@"            
            DELETE FROM {_tablePrefix}
            WHERE id = ?";
        
        await using var command = new MySqlCommand(_deleteFunctionSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.AsGuid.ToString("N")}
            }
        };

        return await command.ExecuteNonQueryAsync() == 1;
    }
    
    private async Task<bool> DoTablesAlreadyExist()
    {
        await using var conn = await CreateOpenConnection(_connectionString);
        
        var sql = $"SELECT 1 FROM {_tablePrefix} LIMIT 1;";

        await using var command = new MySqlCommand(sql, conn);
        try
        {
            await command.ExecuteScalarAsync();
            return true;    
        } catch (MySqlException e)
        {
            if (e.ErrorCode == MySqlErrorCode.NoSuchTable)
                return false;

            throw;
        }
    }
}