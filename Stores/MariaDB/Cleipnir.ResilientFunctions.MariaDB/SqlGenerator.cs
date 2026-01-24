using System.Runtime.Serialization;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Storage.Utils;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class SqlGenerator(string tablePrefix)
{
    public StoreCommand Interrupt(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
                UPDATE {tablePrefix}
                SET 
                    interrupted = TRUE,
                    status = 
                        CASE 
                            WHEN status = {(int)Status.Suspended} THEN {(int)Status.Postponed}
                            ELSE status
                        END,
                    expires = 
                        CASE
                            WHEN status = {(int)Status.Postponed} THEN 0
                            WHEN status = {(int)Status.Suspended} THEN 0
                            ELSE expires
                        END
                WHERE Id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")});";

        return StoreCommand.Create(sql);
    }
    
    private string? _getEffectResultsSql;
    public StoreCommand GetEffects(StoredId storedId)
    {
        _getEffectResultsSql ??= @$"
            SELECT id, effects
            FROM {tablePrefix}
            WHERE id = ?;";

        var command = StoreCommand.Create(
            _getEffectResultsSql,
            values:
            [
                storedId.AsGuid.ToString("N")
            ]
        );
        return command;
    }

    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, effects
            FROM {tablePrefix}
            WHERE id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")});";

        var command = StoreCommand.Create(sql);
        return command;
    }
    
    public record StoredEffectsWithSession(IReadOnlyList<StoredEffect> Effects, SnapshotStorageSession Session);
    public async Task<Dictionary<StoredId, StoredEffectsWithSession>> ReadEffects(MySqlDataReader reader, ReplicaId replicaId, IEnumerable<StoredId> storedIds)
    {
        var session = new SnapshotStorageSession(replicaId);

        var toReturn = new Dictionary<StoredId, StoredEffectsWithSession>();
        
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0).ToGuid().ToStoredId();
            var content = reader.GetValue(1) as byte[];
            var effectsBytes = content == null ? [] : BinaryPacker.Split(content);
            
            foreach (var effectBytes in effectsBytes)
            {
                if (effectBytes == null)
                    throw new SerializationException("Unable to deserialize effect");

                var storedEffect = StoredEffect.Deserialize(effectBytes);
                session.Effects[storedEffect.EffectId] = storedEffect;
            }

            session.RowExists = true;
            session.Version = 0;
            toReturn[id] = new StoredEffectsWithSession(session.Effects.Values.ToList(), session);
        }

        foreach (var storedId in storedIds)
            if (!toReturn.ContainsKey(storedId))
                toReturn[storedId] = new StoredEffectsWithSession(Effects: [], new SnapshotStorageSession(replicaId));

        return toReturn;
    }
    
    public async Task<Dictionary<StoredId, SnapshotStorageSession>> ReadEffectsForMultipleStoredIds(MySqlDataReader reader, IEnumerable<StoredId> storedIds, ReplicaId owner)
    {
        var effects = new Dictionary<StoredId, SnapshotStorageSession>();
        foreach (var storedId in storedIds)
            effects[storedId] = new SnapshotStorageSession(owner);

        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0).ToGuid().ToStoredId();
            var position = reader.GetInt32(1);
            var content = (byte[])reader.GetValue(2);
            var version = reader.GetInt32(3);

            var effectsBytes = BinaryPacker.Split(content);
            var storedEffects = effectsBytes.Select(effectBytes => StoredEffect.Deserialize(effectBytes!)).ToList();

            var session = effects[id];
            foreach (var storedEffect in storedEffects)
                session.Effects[storedEffect.EffectId] = storedEffect;

            session.RowExists = true;
            session.Version = version;
        }

        return effects;
    }
    
    public StoreCommand InsertEffects(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, SnapshotStorageSession session)
    {
        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                session.Effects.Remove(change.EffectId);
            else
                session.Effects[change.EffectId] = change.StoredEffect!;

        var content = session.Serialize();
        session.RowExists = true;
        return StoreCommand.Create(
            $@"UPDATE {tablePrefix}
               SET effects = ?
               WHERE id = ?;",
            [content, storedId.AsGuid.ToString("N")]
        );
    }
    
    private string? _createFunctionSql;
    private string? _createFunctionWithEffectsSql;
    public StoreCommand CreateFunction(
        StoredId storedId,
        FlowInstance humanInstanceId,
        byte[]? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent,
        ReplicaId? owner,
        bool ignoreDuplicate,
        byte[]? effects = null)
    {
        var status = postponeUntil == null ? Status.Executing : Status.Postponed;

        string sql;
        object[] values;

        if (effects == null)
        {
            _createFunctionSql ??= @$"
                INSERT IGNORE INTO {tablePrefix}
                    (id, param_json, status, expires, timestamp, human_instance_id, parent, interrupted, owner)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, 0, ?);";
            sql = _createFunctionSql;
            if (!ignoreDuplicate)
                sql = sql.Replace("IGNORE ", "");

            values = [
                storedId.AsGuid.ToString("N"),
                param ?? (object)DBNull.Value,
                (int)status,
                postponeUntil ?? leaseExpiration,
                timestamp,
                humanInstanceId.Value,
                parent?.AsGuid.ToString("N") ?? (object)DBNull.Value,
                owner?.AsGuid.ToString("N") ?? (object)DBNull.Value,
            ];
        }
        else
        {
            _createFunctionWithEffectsSql ??= @$"
                INSERT IGNORE INTO {tablePrefix}
                    (id, param_json, status, expires, timestamp, human_instance_id, parent, interrupted, owner, effects)
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, 0, ?, ?);";
            sql = _createFunctionWithEffectsSql;
            if (!ignoreDuplicate)
                sql = sql.Replace("IGNORE ", "");

            values = [
                storedId.AsGuid.ToString("N"),
                param ?? (object)DBNull.Value,
                (int)status,
                postponeUntil ?? leaseExpiration,
                timestamp,
                humanInstanceId.Value,
                parent?.AsGuid.ToString("N") ?? (object)DBNull.Value,
                owner?.AsGuid.ToString("N") ?? (object)DBNull.Value,
                effects,
            ];
        }

        return StoreCommand.Create(sql, values);
    }
    
    private string? _succeedFunctionSql;
    private string? _succeedFunctionWithEffectsSql;
    public StoreCommand SucceedFunction(
        StoredId storedId,
        byte[]? result,
        long timestamp,
        Guid expectedReplica,
        byte[]? effects = null)
    {
        if (effects == null)
        {
            _succeedFunctionSql ??= $@"
                UPDATE {tablePrefix}
                SET status = {(int)Status.Succeeded}, result_json = ?, timestamp = ?, owner = NULL
                WHERE id = ?";

            return StoreCommand.Create(
                _succeedFunctionSql,
                values: [
                    result ?? (object)DBNull.Value,
                    timestamp,
                    storedId.AsGuid.ToString("N"),
                    expectedReplica.ToString("N"),
                ]
            );
        }
        else
        {
            _succeedFunctionWithEffectsSql ??= $@"
                UPDATE {tablePrefix}
                SET status = {(int)Status.Succeeded}, result_json = ?, timestamp = ?, owner = NULL, effects = ?
                WHERE id = ?";

            return StoreCommand.Create(
                _succeedFunctionWithEffectsSql,
                values: [
                    result ?? (object)DBNull.Value,
                    timestamp,
                    effects,
                    storedId.AsGuid.ToString("N"),
                    expectedReplica.ToString("N"),
                ]
            );
        }
    }
    
    private string? _postponedFunctionSql;
    private string? _postponedFunctionWithEffectsSql;
    public StoreCommand PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        ReplicaId expectedReplica,
        byte[]? effects = null)
    {
        if (effects == null)
        {
            _postponedFunctionSql ??= $@"
                UPDATE {tablePrefix}
                SET status = {(int)Status.Postponed},
                    expires = CASE WHEN interrupted = 1 THEN 0 ELSE ? END,
                    timestamp = ?,
                    owner = NULL,
                    interrupted = 0
                WHERE
                    id = ? AND
                    owner = ?";

            return StoreCommand.Create(
                _postponedFunctionSql,
                values: [
                    postponeUntil,
                    timestamp,
                    storedId.AsGuid.ToString("N"),
                    expectedReplica.AsGuid.ToString("N"),
                ]
            );
        }
        else
        {
            _postponedFunctionWithEffectsSql ??= $@"
                UPDATE {tablePrefix}
                SET status = {(int)Status.Postponed},
                    expires = CASE WHEN interrupted = 1 THEN 0 ELSE ? END,
                    timestamp = ?,
                    owner = NULL,
                    interrupted = 0,
                    effects = ?
                WHERE
                    id = ? AND
                    owner = ?";

            return StoreCommand.Create(
                _postponedFunctionWithEffectsSql,
                values: [
                    postponeUntil,
                    timestamp,
                    effects,
                    storedId.AsGuid.ToString("N"),
                    expectedReplica.AsGuid.ToString("N"),
                ]
            );
        }
    }
    
    private string? _failFunctionSql;
    private string? _failFunctionWithEffectsSql;
    public StoreCommand FailFunction(
        StoredId storedId,
        StoredException storedException,
        long timestamp,
        ReplicaId expectedReplica,
        byte[]? effects = null)
    {
        if (effects == null)
        {
            _failFunctionSql ??= $@"
                UPDATE {tablePrefix}
                SET status = {(int)Status.Failed}, exception_json = ?, timestamp = ?, owner = NULL
                WHERE
                    id = ? AND
                    owner = ?";

            return StoreCommand.Create(
                _failFunctionSql,
                values: [
                    JsonSerializer.Serialize(storedException),
                    timestamp,
                    storedId.AsGuid.ToString("N"),
                    expectedReplica.AsGuid.ToString("N")
                ]
            );
        }
        else
        {
            _failFunctionWithEffectsSql ??= $@"
                UPDATE {tablePrefix}
                SET status = {(int)Status.Failed}, exception_json = ?, timestamp = ?, owner = NULL, effects = ?
                WHERE
                    id = ? AND
                    owner = ?";

            return StoreCommand.Create(
                _failFunctionWithEffectsSql,
                values: [
                    JsonSerializer.Serialize(storedException),
                    timestamp,
                    effects,
                    storedId.AsGuid.ToString("N"),
                    expectedReplica.AsGuid.ToString("N")
                ]
            );
        }
    }
    
    private string? _suspendFunctionSql;
    private string? _suspendFunctionWithEffectsSql;
    public StoreCommand SuspendFunction(StoredId storedId, long timestamp, ReplicaId expectedReplica, byte[]? effects = null)
    {
        if (effects == null)
        {
            _suspendFunctionSql ??= $@"
                UPDATE {tablePrefix}
                SET status = CASE WHEN interrupted = 1 THEN {(int)Status.Postponed} ELSE {(int)Status.Suspended} END,
                    expires = 0,
                    timestamp = ?,
                    owner = NULL,
                    interrupted = 0
                WHERE id = ? AND owner = ?";

            return StoreCommand.Create(
                _suspendFunctionSql,
                values: [
                    timestamp,
                    storedId.AsGuid.ToString("N"),
                    expectedReplica.AsGuid.ToString("N")
                ]
            );
        }
        else
        {
            _suspendFunctionWithEffectsSql ??= $@"
                UPDATE {tablePrefix}
                SET status = CASE WHEN interrupted = 1 THEN {(int)Status.Postponed} ELSE {(int)Status.Suspended} END,
                    expires = 0,
                    timestamp = ?,
                    owner = NULL,
                    interrupted = 0,
                    effects = ?
                WHERE id = ? AND owner = ?";

            return StoreCommand.Create(
                _suspendFunctionWithEffectsSql,
                values: [
                    timestamp,
                    effects,
                    storedId.AsGuid.ToString("N"),
                    expectedReplica.AsGuid.ToString("N")
                ]
            );
        }
    }

    private string? _restartExecutionSql;
    public StoreCommand RestartExecution(StoredId storedId, ReplicaId replicaId)
    {
        _restartExecutionSql ??= @$"
            UPDATE {tablePrefix}
            SET status = {(int)Status.Executing}, expires = 0, interrupted = FALSE, owner = ?
            WHERE id = ? AND owner IS NULL;

            SELECT
                id,
                param_json,
                status,
                result_json,
                exception_json,
                expires,
                interrupted,
                timestamp,
                human_instance_id,
                parent,
                owner,
                effects
            FROM {tablePrefix}
            WHERE id = ?;";

        var command = StoreCommand.Create(
            _restartExecutionSql,
            values: [
                replicaId.AsGuid.ToString("N"),
                storedId.AsGuid.ToString("N"),
                storedId.AsGuid.ToString("N"),
            ]);
        return command;
    }

    private string? _restartExecutionsSql;
    public StoreCommand RestartExecutions(IReadOnlyList<StoredId> storedIds, ReplicaId replicaId)
    {
        _restartExecutionsSql ??= @$"
            UPDATE {tablePrefix}
            SET status = {(int)Status.Executing}, expires = 0, interrupted = FALSE, owner = ?
            WHERE id IN ({{0}}) AND owner IS NULL;

            SELECT
                id,
                param_json,
                status,
                result_json,
                exception_json,
                expires,
                interrupted,
                timestamp,
                human_instance_id,
                parent,
                owner,
                effects
            FROM {tablePrefix}
            WHERE id IN ({{0}});";

        var idList = storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ");
        var sql = string.Format(_restartExecutionsSql, idList);

        var command = StoreCommand.Create(
            sql,
            values: [ replicaId.AsGuid.ToString("N") ]);
        return command;
    }
    
    public async Task<StoredFlow?> ReadToStoredFunction(StoredId storedId, MySqlDataReader reader)
    {
        const int idIndex = 0;
        const int paramIndex = 1;
        const int statusIndex = 2;
        const int resultIndex = 3;
        const int exceptionIndex = 4;
        const int expiresIndex = 5;
        const int interruptedIndex = 6;
        const int timestampIndex = 7;
        const int humanInstanceIdIndex = 8;
        const int parentIndex = 9;
        const int ownerIndex = 10;

        while (await reader.ReadAsync())
        {
            var id = reader.GetString(idIndex).ToGuid().ToStoredId();
            var hasParam = !await reader.IsDBNullAsync(paramIndex);
            var hasResult = !await reader.IsDBNullAsync(resultIndex);
            var hasError = !await reader.IsDBNullAsync(exceptionIndex);
            var hasParent = !await reader.IsDBNullAsync(parentIndex);
            var hasOwner = !await reader.IsDBNullAsync(ownerIndex);
            var storedException = hasError
                ? JsonSerializer.Deserialize<StoredException>(reader.GetString(exceptionIndex))
                : null;
            return new StoredFlow(
                id,
                InstanceId: reader.GetString(humanInstanceIdIndex),
                hasParam ? (byte[]) reader.GetValue(paramIndex) : null,
                Status: (Status) reader.GetInt32(statusIndex),
                storedException,
                Expires: reader.GetInt64(expiresIndex),
                Timestamp: reader.GetInt64(timestampIndex),
                Interrupted: reader.GetBoolean(interruptedIndex),
                ParentId: hasParent ? reader.GetString(parentIndex).ToGuid().ToStoredId() : null,
                OwnerId: hasOwner ? reader.GetString(ownerIndex).ParseToReplicaId() : null,
                StoredType: storedId.Type
            );
        }

        return null;
    }

    public async Task<(StoredFlow?, byte[]?)> ReadToStoredFunctionWithEffects(StoredId storedId, MySqlDataReader reader)
    {
        const int idIndex = 0;
        const int paramIndex = 1;
        const int statusIndex = 2;
        const int resultIndex = 3;
        const int exceptionIndex = 4;
        const int expiresIndex = 5;
        const int interruptedIndex = 6;
        const int timestampIndex = 7;
        const int humanInstanceIdIndex = 8;
        const int parentIndex = 9;
        const int ownerIndex = 10;
        const int effectsIndex = 11;

        while (await reader.ReadAsync())
        {
            var id = reader.GetString(idIndex).ToGuid().ToStoredId();
            var hasParam = !await reader.IsDBNullAsync(paramIndex);
            var hasResult = !await reader.IsDBNullAsync(resultIndex);
            var hasError = !await reader.IsDBNullAsync(exceptionIndex);
            var hasParent = !await reader.IsDBNullAsync(parentIndex);
            var hasOwner = !await reader.IsDBNullAsync(ownerIndex);
            var hasEffects = !await reader.IsDBNullAsync(effectsIndex);
            var storedException = hasError
                ? JsonSerializer.Deserialize<StoredException>(reader.GetString(exceptionIndex))
                : null;

            var effectsBytes = hasEffects ? (byte[])reader.GetValue(effectsIndex) : null;

            var storedFlow = new StoredFlow(
                id,
                InstanceId: reader.GetString(humanInstanceIdIndex),
                hasParam ? (byte[])reader.GetValue(paramIndex) : null,
                Status: (Status)reader.GetInt32(statusIndex),
                storedException,
                Expires: reader.GetInt64(expiresIndex),
                Timestamp: reader.GetInt64(timestampIndex),
                Interrupted: reader.GetBoolean(interruptedIndex),
                ParentId: hasParent ? reader.GetString(parentIndex).ToGuid().ToStoredId() : null,
                OwnerId: hasOwner ? reader.GetString(ownerIndex).ParseToReplicaId() : null,
                StoredType: storedId.Type
            );

            return (storedFlow, effectsBytes);
        }

        return (null, null);
    }
    
    public StoreCommand AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages)
    {
        var sql = @$"
            INSERT INTO {tablePrefix}_messages
                (id, position, content)
            VALUES
                 {"(?, ?, ?)".Replicate(messages.Count).StringJoin($",{Environment.NewLine}")};";

        var command = StoreCommand.Create(sql);
        foreach (var (storedId, (messageContent, messageType, _, idempotencyKey), position) in messages)
        {
            command.AddParameter(storedId.AsGuid.ToString("N"));
            command.AddParameter(position);
            var content = BinaryPacker.Pack(
                messageContent,
                messageType,
                idempotencyKey?.ToUtf8Bytes()
            );
            command.AddParameter(content);
        }

        return command;
    }
    
    private string? _getMessagesSql;
    public StoreCommand GetMessages(StoredId storedId, long skip)
    {
        _getMessagesSql ??= @$"
            SELECT content, position
            FROM {tablePrefix}_messages
            WHERE id = ? AND position >= ?
            ORDER BY position ASC;";

        var command = StoreCommand.Create(
            _getMessagesSql,
            values:
            [
                storedId.AsGuid.ToString("N"),
                skip
            ]
        );
        return command;
    }

    public StoreCommand GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
    {
        var positionsClause = skipPositions.Select(p => p.ToString()).StringJoin(", ");
        var sql = @$"
            SELECT content, position
            FROM {tablePrefix}_messages
            WHERE id = ? AND position NOT IN ({positionsClause})
            ORDER BY position;";

        var command = StoreCommand.Create(
            sql,
            values: [storedId.AsGuid.ToString("N")]
        );
        return command;
    }

    public async Task<IReadOnlyList<(byte[] content, long position)>> ReadMessages(MySqlDataReader reader)
    {
        var messages = new List<(byte[], long)>();
        while (await reader.ReadAsync())
        {
            var content = (byte[]) reader.GetValue(0);
            var position = reader.GetInt64(1);
            messages.Add((content, position));
        }

        return messages;
    }
    
    public StoreCommand GetMessages(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, position, content
            FROM {tablePrefix}_messages
            WHERE id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")})
            ORDER BY position;";

        var command = StoreCommand.Create(sql);
        return command;
    }
    
    public async Task<Dictionary<StoredId, List<(byte[] content, long position)>>> ReadStoredIdsMessages(MySqlDataReader reader)
    {
        var messages = new Dictionary<StoredId, List<(byte[] content, long position)>>();

        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0).ToGuid().ToStoredId();
            var position = reader.GetInt64(1);
            var content = (byte[]) reader.GetValue(2);

            if (!messages.ContainsKey(id))
                messages[id] = new List<(byte[], long)>();

            messages[id].Add((content, position));
        }

        return messages.ToDictionary(
            kv => kv.Key,
            kv => kv.Value
                .OrderBy(m => m.position)
                .ToList());
    }

    public StoreCommand DeleteMessages(StoredId storedId, IEnumerable<long> positions)
    {
        var positionsList = positions.ToList();

        var sql = @$"
                DELETE FROM {tablePrefix}_messages
                WHERE id = ? AND position IN ({string.Join(", ", positionsList.Select(_ => "?"))})";

        var command = StoreCommand.Create(sql);
        command.AddParameter(storedId.AsGuid.ToString("N"));
        foreach (var position in positionsList)
            command.AddParameter(position);

        return command;
    }
}