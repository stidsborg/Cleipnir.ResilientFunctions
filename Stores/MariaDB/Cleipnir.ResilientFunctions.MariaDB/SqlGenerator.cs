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
            SELECT position, status, result, exception, effect_id
            FROM {tablePrefix}_effects
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
    
    public async Task<IReadOnlyList<StoredEffect>> ReadEffects(MySqlDataReader reader)
        => (await ReadEffectsWithPositions(reader)).Select(e => e.Effect).ToList();

    public async Task<IReadOnlyList<StoredEffectWithPosition>> ReadEffectsWithPositions(MySqlDataReader reader)
    {
        var functions = new List<StoredEffectWithPosition>();
        while (await reader.ReadAsync())
        {
            var position = reader.GetInt64(0);
            var status = (WorkStatus) reader.GetInt32(1);
            var result = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
            var exception = reader.IsDBNull(3) ? null : reader.GetString(3);
            var effectId = reader.GetString(4);
            functions.Add(
                new StoredEffectWithPosition(
                    new StoredEffect(
                        EffectId.Deserialize(effectId),
                        status,
                        result,
                        JsonHelper.FromJson<StoredException>(exception)
                    ),
                    position
                )
            );
        }

        return functions;
    }
    
    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, position, status, result, exception, effect_id
            FROM {tablePrefix}_effects
            WHERE id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")});";

        var command = StoreCommand.Create(sql);
        return command;
    }
    
    public async Task<Dictionary<StoredId, List<StoredEffectWithPosition>>> ReadEffectsForMultipleStoredIds(MySqlDataReader reader, IEnumerable<StoredId> storedIds)
    {
        var storedEffects = new Dictionary<StoredId, List<StoredEffectWithPosition>>();
        foreach (var storedId in storedIds)
            storedEffects[storedId] = new List<StoredEffectWithPosition>();

        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0).ToGuid().ToStoredId();
            var position = reader.GetInt64(1);
            var status = (WorkStatus) reader.GetInt32(2);
            var result = reader.IsDBNull(3) ? null : (byte[]) reader.GetValue(3);
            var exception = reader.IsDBNull(4) ? null : reader.GetString(4);
            var effectId = reader.GetString(5);

            storedEffects[id].Add(
                new StoredEffectWithPosition(
                    new StoredEffect(
                        EffectId.Deserialize(effectId),
                        status,
                        result,
                        JsonHelper.FromJson<StoredException>(exception)
                    ),
                    position
                )
            );
        }

        return storedEffects;
    }
    
    public StoreCommand? UpdateEffects(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, PositionsStorageSession session)
    {
        var storeCommands = new List<StoreCommand>(2);

        // DELETES
        {
            var positionsToDelete = changes
                .Select(c =>
                    session.Positions.ContainsKey(c.EffectId.Serialize())
                        ? session.Positions[c.EffectId.Serialize()]
                        : -1)
                .Where(p => p != -1)
                .ToList();

            if (positionsToDelete.Any())
            {
                var removeSql = @$"
                DELETE FROM {tablePrefix}_effects
                WHERE id = '{storedId.AsGuid:N}' AND position IN ({positionsToDelete.Select(p => p.ToString()).StringJoin(separator: ", ")});";
                storeCommands.Add(StoreCommand.Create(removeSql));
            }
        }

        // INSERT and UPDATES
        {
            var insertsAndUpdates = changes
                .Where(c => c.Operation == CrudOperation.Insert || c.Operation == CrudOperation.Update)
                .ToList();

            foreach (var (_, effectId, _, storedEffect) in insertsAndUpdates)
            {
                var position = session.Add(effectId.Serialize());

                var sql = $@"
                INSERT INTO {tablePrefix}_effects
                    (id, position, status, result, exception, effect_id)
                VALUES
                    (?, ?, ?, ?, ?, ?);";

                var command = StoreCommand.Create(sql);
                command.AddParameter(storedId.AsGuid.ToString("N"));
                command.AddParameter(position);
                command.AddParameter((int)storedEffect!.WorkStatus);
                command.AddParameter(storedEffect.Result ?? (object)DBNull.Value);
                command.AddParameter(JsonHelper.ToJson(storedEffect.StoredException) ?? (object)DBNull.Value);
                command.AddParameter(storedEffect.EffectId.Serialize().Value);

                storeCommands.Add(command);
            }
        }

        return storeCommands.Count == 0
            ? null
            : StoreCommand.Merge(storeCommands)!;
    }
    
    private string? _createFunctionSql;
    public StoreCommand CreateFunction(
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
        _createFunctionSql ??= @$"
            INSERT IGNORE INTO {tablePrefix}
                (id, param_json, status, expires, timestamp, human_instance_id, parent, interrupted, owner)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, 0, ?);";
        var status = postponeUntil == null ? Status.Executing : Status.Postponed;

        var sql = _createFunctionSql;
        if (!ignoreDuplicate)
            sql = sql.Replace("IGNORE ", "");
        
        return StoreCommand.Create(
            sql,
            values: [
                storedId.AsGuid.ToString("N"),
                param ?? (object)DBNull.Value,
                (int)status,
                postponeUntil ?? leaseExpiration,
                timestamp,
                humanInstanceId.Value,
                parent?.Serialize() ?? (object)DBNull.Value,
                owner?.AsGuid.ToString("N") ?? (object)DBNull.Value,
            ]
        );
    }
    
    private string? _succeedFunctionSql;
    public StoreCommand SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        Guid expectedReplica)
    {
        _succeedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Succeeded}, result_json = ?, timestamp = ?, owner = NULL
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
            SET status = {(int) Status.Failed}, exception_json = ?, timestamp = ?, owner = NULL
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
                HumanInstanceId: reader.GetString(humanInstanceIdIndex),
                hasParam ? (byte[]) reader.GetValue(paramIndex) : null,
                Status: (Status) reader.GetInt32(statusIndex),
                Result: hasResult ? (byte[]) reader.GetValue(resultIndex) : null, 
                storedException, 
                Expires: reader.GetInt64(expiresIndex),
                Interrupted: reader.GetBoolean(interruptedIndex),
                Timestamp: reader.GetInt64(timestampIndex),
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
                (id, position, message_json, message_type, idempotency_key)
            VALUES 
                 {"(?, ?, ?, ?, ?)".Replicate(messages.Count).StringJoin($",{Environment.NewLine}")};";

        var command = StoreCommand.Create(sql);
        foreach (var (storedId, (messageContent, messageType, idempotencyKey), position) in messages)
        {
            command.AddParameter(storedId.AsGuid.ToString("N"));
            command.AddParameter(position);
            command.AddParameter(messageContent);
            command.AddParameter(messageType);
            command.AddParameter(idempotencyKey ?? (object)DBNull.Value);
        }

        return command;
    }
    
    private string? _getMessagesSql;
    public StoreCommand GetMessages(StoredId storedId, int skip)
    {
        _getMessagesSql ??= @$"    
            SELECT message_json, message_type, idempotency_key
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
    
    public async Task<IReadOnlyList<StoredMessage>> ReadMessages(MySqlDataReader reader)
    {
        var storedMessages = new List<StoredMessage>();
        while (await reader.ReadAsync())
        {
            var messageJson = (byte[]) reader.GetValue(0);
            var messageType = (byte[]) reader.GetValue(1);
            var idempotencyKey = reader.IsDBNull(2) ? null : reader.GetString(2);
            storedMessages.Add(new StoredMessage(messageJson, messageType, idempotencyKey));
        }

        return storedMessages;
    }
    
    public StoreCommand GetMessages(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"    
            SELECT id, position, message_json, message_type, idempotency_key
            FROM {tablePrefix}_messages
            WHERE id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")});";

        var command = StoreCommand.Create(sql);
        return command;
    }
    
    public async Task<Dictionary<StoredId, List<StoredMessage>>> ReadStoredIdsMessages(MySqlDataReader reader)
    {
        var storedMessages = new Dictionary<StoredId, List<StoredMessageWithPosition>>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0).ToGuid().ToStoredId();
            var position = reader.GetInt32(1);
            var messageJson = (byte[]) reader.GetValue(2);
            var messageType = (byte[]) reader.GetValue(3);
            var idempotencyKey = reader.IsDBNull(4) ? null : reader.GetString(4);
            if (!storedMessages.ContainsKey(id))
                storedMessages[id] = new List<StoredMessageWithPosition>();
            
            storedMessages[id].Add(new StoredMessageWithPosition(new StoredMessage(messageJson, messageType, idempotencyKey), position));
        }

        return storedMessages.ToDictionary(kv => kv.Key, kv => kv.Value.OrderBy(m => m.Position).Select(m => m.StoredMessage).ToList());
    }
}