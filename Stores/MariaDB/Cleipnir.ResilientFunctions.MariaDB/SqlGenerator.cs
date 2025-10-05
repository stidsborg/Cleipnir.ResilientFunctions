using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
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
            SELECT id_hash, status, result, exception, effect_id
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
    {
        var functions = new List<StoredEffect>();
        while (await reader.ReadAsync())
        {
            var idHash = reader.GetString(0);
            var status = (WorkStatus) reader.GetInt32(1);
            var result = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
            var exception = reader.IsDBNull(3) ? null : reader.GetString(3);
            var effectId = reader.GetString(4);
            functions.Add(
                new StoredEffect(
                    EffectId.Deserialize(effectId),
                    new StoredEffectId(Guid.Parse(idHash)),
                    status,
                    result,
                    StoredException: JsonHelper.FromJson<StoredException>(exception)
                )
            );
        }

        return functions;
    }
    
    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, id_hash, status, result, exception, effect_id
            FROM {tablePrefix}_effects
            WHERE id IN ({storedIds.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")});";

        var command = StoreCommand.Create(sql);
        return command;
    }
    
    public async Task<Dictionary<StoredId, List<StoredEffect>>> ReadEffectsForMultipleStoredIds(MySqlDataReader reader)
    {
        var storedEffects = new Dictionary<StoredId, List<StoredEffect>>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0).ToGuid().ToStoredId();
            var idHash = reader.GetString(1);
            var status = (WorkStatus) reader.GetInt32(2);
            var result = reader.IsDBNull(3) ? null : (byte[]) reader.GetValue(3);
            var exception = reader.IsDBNull(4) ? null : reader.GetString(5);
            var effectId = reader.GetString(5);

            if (!storedEffects.ContainsKey(id))
                storedEffects[id] = new List<StoredEffect>();

            storedEffects[id].Add(
                new StoredEffect(
                    EffectId.Deserialize(effectId),
                    new StoredEffectId(Guid.Parse(idHash)),
                    status,
                    result,
                    StoredException: JsonHelper.FromJson<StoredException>(exception)
                )
            );
        }

        return storedEffects;
    }
    
    public StoreCommand UpdateEffects(IReadOnlyList<StoredEffectChange> changes)
    {
        var upsertCommand = default(StoreCommand);
        
        if (changes.Any(c => c.Operation == CrudOperation.Update || c.Operation == CrudOperation.Insert))
        {
            var upserts = changes
                .Where(c => c.Operation == CrudOperation.Update || c.Operation == CrudOperation.Insert)
                .Select(c => new
                {
                    Instance = c.StoredId.AsGuid, 
                    IdHash = c.EffectId.Value,
                    WorkStatus = (int)c.StoredEffect!.WorkStatus, 
                    Result = c.StoredEffect!.Result,
                    Exception = c.StoredEffect!.StoredException, 
                    EffectId = c.StoredEffect!.EffectId
                })
                .ToList();
        
            var setSql = $@"
                INSERT INTO {tablePrefix}_effects 
                    (id, id_hash, status, result, exception, effect_id)
                VALUES
                    {"(?, ?, ?, ?, ?, ?)".Replicate(upserts.Count).StringJoin(", ")}  
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status), result = VALUES(result), exception = VALUES(exception);";

            upsertCommand = StoreCommand.Create(setSql);
            foreach (var upsert in upserts)
            {
                upsertCommand.AddParameter(upsert.Instance.ToString("N"));
                upsertCommand.AddParameter(upsert.IdHash.ToString("N"));
                upsertCommand.AddParameter(upsert.WorkStatus);
                upsertCommand.AddParameter(upsert.Result ?? (object) DBNull.Value);
                upsertCommand.AddParameter(JsonHelper.ToJson(upsert.Exception) ?? (object) DBNull.Value);
                upsertCommand.AddParameter(upsert.EffectId.Serialize());
            }    
        }

        var removeCommand = default(StoreCommand);
        if (changes.Any(c => c.Operation == CrudOperation.Delete))
        {
            var removes = changes
                .Where(c => c.Operation == CrudOperation.Delete)
                .Select(c => new { Id = c.StoredId.AsGuid, IdHash = c.EffectId.Value })
                .GroupBy(a => a.Id, a => a.IdHash)
                .ToList();
            var predicates = removes
                .Select(r =>
                    $"(id = '{r.Key:N}' AND id_hash IN ({r.Select(id => $"'{id:N}'").StringJoin(", ")}))")
                .StringJoin($" OR {Environment.NewLine}");
            var removeSql = @$"
            DELETE FROM {tablePrefix}_effects 
            WHERE {predicates}";
            
            removeCommand = StoreCommand.Create(removeSql);
        }
        
        return StoreCommand.Merge([upsertCommand, removeCommand])!;
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
        bool ignoreInterrupted,
        ReplicaId expectedReplica)
    {
        _postponedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Postponed}, expires = ?, timestamp = ?, owner = NULL
            WHERE 
                id = ? AND 
                owner = ? AND
                interrupted = 0";

        var sql = _postponedFunctionSql;
        if (ignoreInterrupted)
            sql = sql.Replace("interrupted = 0", "1 = 1");

        return StoreCommand.Create(
            sql,
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
            SET status = {(int) Status.Suspended}, timestamp = ?, owner = NULL
            WHERE 
                  id = ? AND 
                  owner = ? AND
                  NOT interrupted";

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