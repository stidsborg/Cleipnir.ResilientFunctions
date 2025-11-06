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
            SELECT id, position, content, version
            FROM {tablePrefix}_state
            WHERE id = ? AND position = 0;";

        var command = StoreCommand.Create(
            _getEffectResultsSql,
            values:
            [
                storedId.AsGuid.ToString("N")
            ]
        );
        return command;
    }

    private string? _getInputOutputSql;
    public StoreCommand GetInputOutput(StoredId storedId)
    {
        _getInputOutputSql ??= @$"
            SELECT id, param_json, result_json, exception_json, human_instance_id, parent
            FROM {tablePrefix}_inputoutput
            WHERE id = ?;";

        return StoreCommand.Create(
            _getInputOutputSql,
            values: [ storedId.AsGuid.ToString("N") ]);
    }

    private string? _getInputOutputsSql;
    public StoreCommand GetInputOutput(IEnumerable<StoredId> storedIds)
    {
        _getInputOutputsSql ??= @$"
            SELECT id, param_json, result_json, exception_json, human_instance_id, parent
            FROM {tablePrefix}_inputoutput
            WHERE id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")});";

        return StoreCommand.Create(_getInputOutputsSql);
    }

    public async Task<StoredInputOutput?> ReadInputOutput(MySqlDataReader reader)
    {
        const int idIndex = 0;
        const int paramIndex = 1;
        const int resultIndex = 2;
        const int exceptionIndex = 3;
        const int humanInstanceIdIndex = 4;
        const int parentIndex = 5;

        while (await reader.ReadAsync())
        {
            var id = reader.GetString(idIndex).ToGuid().ToStoredId();
            var hasParam = !await reader.IsDBNullAsync(paramIndex);
            var hasResult = !await reader.IsDBNullAsync(resultIndex);
            var hasException = !await reader.IsDBNullAsync(exceptionIndex);
            var hasParent = !await reader.IsDBNullAsync(parentIndex);

            return new StoredInputOutput(
                id,
                hasParam ? (byte[])reader.GetValue(paramIndex) : null,
                hasResult ? (byte[])reader.GetValue(resultIndex) : null,
                hasException ? reader.GetString(exceptionIndex) : null,
                reader.GetString(humanInstanceIdIndex),
                hasParent ? StoredId.Deserialize(reader.GetString(parentIndex)) : null
            );
        }

        return null;
    }

    public async Task<Dictionary<StoredId, StoredInputOutput>> ReadInputOutputs(MySqlDataReader reader)
    {
        const int idIndex = 0;
        const int paramIndex = 1;
        const int resultIndex = 2;
        const int exceptionIndex = 3;
        const int humanInstanceIdIndex = 4;
        const int parentIndex = 5;

        var result = new Dictionary<StoredId, StoredInputOutput>();

        while (await reader.ReadAsync())
        {
            var id = reader.GetString(idIndex).ToGuid().ToStoredId();
            var hasParam = !await reader.IsDBNullAsync(paramIndex);
            var hasResult = !await reader.IsDBNullAsync(resultIndex);
            var hasException = !await reader.IsDBNullAsync(exceptionIndex);
            var hasParent = !await reader.IsDBNullAsync(parentIndex);

            result[id] = new StoredInputOutput(
                id,
                hasParam ? (byte[])reader.GetValue(paramIndex) : null,
                hasResult ? (byte[])reader.GetValue(resultIndex) : null,
                hasException ? reader.GetString(exceptionIndex) : null,
                reader.GetString(humanInstanceIdIndex),
                hasParent ? StoredId.Deserialize(reader.GetString(parentIndex)) : null
            );
        }

        return result;
    }
    
    public record StoredEffectsWithSession(IReadOnlyList<StoredEffect> Effects, SnapshotStorageSession Session);
    public async Task<StoredEffectsWithSession> ReadEffects(MySqlDataReader reader)
    {
        var effects = new List<StoredEffect>();
        var session = new SnapshotStorageSession();

        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0);
            var position = reader.GetInt32(1);
            var content = (byte[])reader.GetValue(2);
            var version = reader.GetInt32(3);
            var effectsBytes = BinaryPacker.Split(content);
            foreach (var effectBytes in effectsBytes)
            {
                if (effectBytes == null)
                    throw new SerializationException("Unable to deserialize effect");

                var storedEffect = StoredEffect.Deserialize(effectBytes);
                effects.Add(storedEffect);
                session.Effects[storedEffect.EffectId] = storedEffect;
            }

            session.RowExists = true;
            session.Version = version;
        }

        return new StoredEffectsWithSession(effects, session);
    }
    
    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, position, content, version
            FROM {tablePrefix}_state
            WHERE id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")}) AND position = 0;";

        var command = StoreCommand.Create(sql);
        return command;
    }
    
    public async Task<Dictionary<StoredId, SnapshotStorageSession>> ReadEffectsForMultipleStoredIds(MySqlDataReader reader, IEnumerable<StoredId> storedIds)
    {
        var effects = new Dictionary<StoredId, SnapshotStorageSession>();
        foreach (var storedId in storedIds)
            effects[storedId] = new SnapshotStorageSession();

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
            $@"INSERT INTO {tablePrefix}_state
                     (id, position, content, version)
                   VALUES
                     (?, 0, ?, 0);",
            [storedId.AsGuid.ToString("N"), content]
        );
    }
    
    private string? _createFunctionInputOutputSql;
    private string? _createFunctionMainSql;
    public IEnumerable<StoreCommand> CreateFunction(
        StoredId storedId,
        FlowInstance humanInstanceId,
        byte[]? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent,
        ReplicaId? owner,
        bool ignoreDuplicate)
    {
        _createFunctionInputOutputSql ??= @$"
            INSERT IGNORE INTO {tablePrefix}_inputoutput
                (id, param_json, result_json, exception_json, human_instance_id, parent)
            VALUES
                (?, ?, NULL, NULL, ?, ?);";

        _createFunctionMainSql ??= @$"
            INSERT IGNORE INTO {tablePrefix}
                (id, status, expires, timestamp, interrupted, owner)
            VALUES
               (?, ?, ?, ?, 0, ?);";
        var status = postponeUntil == null ? Status.Executing : Status.Postponed;

        var inputOutputSql = _createFunctionInputOutputSql;
        var mainSql = _createFunctionMainSql;
        if (!ignoreDuplicate)
        {
            inputOutputSql = inputOutputSql.Replace("IGNORE ", "");
            mainSql = mainSql.Replace("IGNORE ", "");
        }

        yield return StoreCommand.Create(
            inputOutputSql,
            values: [
                storedId.AsGuid.ToString("N"),
                param ?? (object)DBNull.Value,
                humanInstanceId.Value,
                parent?.Serialize() ?? (object)DBNull.Value,
            ]
        );

        yield return StoreCommand.Create(
            mainSql,
            values: [
                storedId.AsGuid.ToString("N"),
                (int)status,
                postponeUntil ?? leaseExpiration,
                timestamp,
                owner?.AsGuid.ToString("N") ?? (object)DBNull.Value,
            ]
        );
    }

    public StoreCommand BulkScheduleFunctions(IdWithParam idWithParam, StoredId? parent)
    {
        var now = DateTime.UtcNow.Ticks;
        var inputOutputSql = @$"
            INSERT IGNORE INTO {tablePrefix}_inputoutput
                (id, param_json, result_json, exception_json, human_instance_id, parent)
            VALUES
                ('{idWithParam.StoredId.AsGuid:N}', {(idWithParam.Param == null ? "NULL" : $"x'{Convert.ToHexString(idWithParam.Param)}'")}, NULL, NULL, '{idWithParam.HumanInstanceId.EscapeString()}', {(parent == null ? "NULL" : $"'{parent}'")});";

        var mainSql = @$"
            INSERT IGNORE INTO {tablePrefix}
                (id, status, expires, timestamp, interrupted, owner)
            VALUES
                ('{idWithParam.StoredId.AsGuid:N}', {(int)Status.Postponed}, 0, {now}, 0, NULL);";

        return StoreCommand.Create(inputOutputSql + Environment.NewLine + mainSql);
    }
    
    private string? _succeedFunctionSql;
    public StoreCommand SucceedFunction(
        StoredId storedId,
        byte[]? result,
        long timestamp,
        Guid expectedReplica)
    {
        _succeedFunctionSql ??= $@"
            UPDATE {tablePrefix}_inputoutput
            SET result_json = ?
            WHERE id = ?;

            UPDATE {tablePrefix}
            SET status = {(int)Status.Succeeded}, timestamp = ?, owner = NULL
            WHERE id = ? AND owner = ?;";

        return StoreCommand.Create(
            _succeedFunctionSql,
            values: [
                result ?? (object)DBNull.Value,
                storedId.AsGuid.ToString("N"),
                timestamp,
                storedId.AsGuid.ToString("N"),
                expectedReplica.ToString("N"),
            ]
        );
    }
    
    private string? _postponedFunctionSql;
    public StoreCommand PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        ReplicaId expectedReplica)
    {
        _postponedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Postponed},
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
    
    private string? _failFunctionSql;
    public StoreCommand FailFunction(
        StoredId storedId,
        StoredException storedException,
        long timestamp,
        ReplicaId expectedReplica)
    {
        _failFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int)Status.Failed}, timestamp = ?, owner = NULL
            WHERE id = ? AND owner = ?;

            UPDATE {tablePrefix}_inputoutput
            SET exception_json = ?
            WHERE id = ?;";

        return StoreCommand.Create(
            _failFunctionSql,
            values: [
                timestamp,
                storedId.AsGuid.ToString("N"),
                expectedReplica.AsGuid.ToString("N"),
                JsonSerializer.Serialize(storedException),
                storedId.AsGuid.ToString("N"),
            ]
        );
    }
    
    private string? _suspendFunctionSql;
    public StoreCommand SuspendFunction(StoredId storedId, long timestamp, ReplicaId expectedReplica)
    {
        _suspendFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = CASE WHEN interrupted = 1 THEN {(int) Status.Postponed} ELSE {(int) Status.Suspended} END,
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

    private string? _restartExecutionSql;
    public StoreCommand RestartExecution(StoredId storedId, ReplicaId replicaId)
    {
        _restartExecutionSql ??= @$"
            UPDATE {tablePrefix}
            SET status = {(int)Status.Executing}, expires = 0, interrupted = FALSE, owner = ?
            WHERE id = ? AND owner IS NULL;

            SELECT
                id,
                expires,
                interrupted,
                timestamp,
                owner,
                status
            FROM {tablePrefix}
            WHERE id = ?;

            SELECT
                id,
                param_json,
                result_json,
                exception_json,
                human_instance_id,
                parent
            FROM {tablePrefix}_inputoutput
            WHERE id = ?;";

        var command = StoreCommand.Create(
            _restartExecutionSql,
            values: [
                replicaId.AsGuid.ToString("N"),
                storedId.AsGuid.ToString("N"),
                storedId.AsGuid.ToString("N"),
                storedId.AsGuid.ToString("N"),
            ]);
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
                ParentId: hasParent ? StoredId.Deserialize(reader.GetString(parentIndex)) : null,
                OwnerId: hasOwner ? reader.GetString(ownerIndex).ParseToReplicaId() : null,
                StoredType: storedId.Type
            );
        }

        return null;
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
            WHERE id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")});";

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
}