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
        var conditionals = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(group => $"(type = {group.Key} AND instance IN ({group.Select(i => $"'{i:N}'").StringJoin(", ")}))")
            .StringJoin(" OR ");

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
                WHERE {conditionals};";

        return StoreCommand.Create(sql);
    }
    
    private string? _getEffectResultsSql;
    public StoreCommand GetEffects(StoredId storedId)
    {
        _getEffectResultsSql ??= @$"
            SELECT id_hash, status, result, exception, effect_id
            FROM {tablePrefix}_effects
            WHERE type = ? AND instance = ?;";

        var command = StoreCommand.Create(
            _getEffectResultsSql,
            values:
            [
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N")
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

    
    public StoreCommand UpdateEffects(IReadOnlyList<StoredEffectChange> changes)
    {
        var upsertCommand = default(StoreCommand);
        
        if (changes.Any(c => c.Operation == CrudOperation.Insert))
        {
            var upserts = changes
                .Where(c => c.Operation == CrudOperation.Insert)
                .Select(c => new
                {
                    Type = c.StoredId.Type.Value, 
                    Instance = c.StoredId.Instance.Value, 
                    IdHash = c.EffectId.Value,
                    WorkStatus = (int)c.StoredEffect!.WorkStatus, 
                    Result = c.StoredEffect!.Result,
                    Exception = c.StoredEffect!.StoredException, 
                    EffectId = c.StoredEffect!.EffectId
                })
                .ToList();
        
            var setSql = $@"
                INSERT INTO {tablePrefix}_effects 
                    (type, instance, id_hash, status, result, exception, effect_id)
                VALUES
                    {"(?, ?, ?, ?, ?, ?, ?)".Replicate(upserts.Count).StringJoin(", ")};";

            upsertCommand = StoreCommand.Create(setSql);
            foreach (var upsert in upserts)
            {
                upsertCommand.AddParameter(upsert.Type);
                upsertCommand.AddParameter(upsert.Instance.ToString("N"));
                upsertCommand.AddParameter(upsert.IdHash.ToString("N"));
                upsertCommand.AddParameter(upsert.WorkStatus);
                upsertCommand.AddParameter(upsert.Result ?? (object) DBNull.Value);
                upsertCommand.AddParameter(JsonHelper.ToJson(upsert.Exception) ?? (object) DBNull.Value);
                upsertCommand.AddParameter(upsert.EffectId.Serialize());
            }    
        } 
        
        if (changes.Any(c => c.Operation == CrudOperation.Update))
        {
            var upserts = changes
                .Where(c => c.Operation == CrudOperation.Update)
                .Select(c => new
                {
                    Type = c.StoredId.Type.Value, 
                    Instance = c.StoredId.Instance.Value, 
                    IdHash = c.EffectId.Value,
                    WorkStatus = (int)c.StoredEffect!.WorkStatus, 
                    Result = c.StoredEffect!.Result,
                    Exception = c.StoredEffect!.StoredException, 
                    EffectId = c.StoredEffect!.EffectId
                })
                .ToList();
        
            var setSql = $@"
                INSERT INTO {tablePrefix}_effects 
                    (type, instance, id_hash, status, result, exception, effect_id)
                VALUES
                    {"(?, ?, ?, ?, ?, ?, ?)".Replicate(upserts.Count).StringJoin(", ")}  
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status), result = VALUES(result), exception = VALUES(exception);";

            upsertCommand = StoreCommand.Create(setSql);
            foreach (var upsert in upserts)
            {
                upsertCommand.AddParameter(upsert.Type);
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
                .Select(c => new { Type = c.StoredId.Type.Value, Instance = c.StoredId.Instance.Value, IdHash = c.EffectId.Value })
                .GroupBy(a => new {a.Type, a.Instance }, a => a.IdHash)
                .ToList();
            var predicates = removes
                .Select(r =>
                    $"(type = {r.Key.Type} AND instance = '{r.Key.Instance:N}' AND id_hash IN ({r.Select(id => $"'{id:N}'").StringJoin(", ")}))")
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
                (type, instance, param_json, status, epoch, expires, timestamp, human_instance_id, parent, interrupted, owner)
            VALUES
                (?, ?, ?, ?, 0, ?, ?, ?, ?, 0, ?);";
        var status = postponeUntil == null ? Status.Executing : Status.Postponed;

        var sql = _createFunctionSql;
        if (!ignoreDuplicate)
            sql = sql.Replace("IGNORE ", "");
        
        return StoreCommand.Create(
            sql,
            values: [
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
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
        int expectedEpoch)
    {
        _succeedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Succeeded}, result_json = ?, timestamp = ?, epoch = ?
            WHERE 
                type = ? AND 
                instance = ? AND 
                epoch = ?";

        return StoreCommand.Create(
            _succeedFunctionSql,
            values: [
                result ?? (object)DBNull.Value,
                timestamp,
                expectedEpoch,
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                expectedEpoch,
            ]
        );
    }
    
    private string? _postponedFunctionSql;
    public StoreCommand PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        bool ignoreInterrupted,
        int expectedEpoch)
    {
        _postponedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Postponed}, expires = ?, timestamp = ?, epoch = ?
            WHERE 
                type = ? AND 
                instance = ? AND 
                epoch = ? AND
                interrupted = 0";

        var sql = _postponedFunctionSql;
        if (ignoreInterrupted)
            sql = sql.Replace("interrupted = 0", "1 = 1");

        return StoreCommand.Create(
            sql,
            values: [
                postponeUntil,
                timestamp,
                expectedEpoch,
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                expectedEpoch,
            ]
        );
    }
    
    private string? _failFunctionSql;
    public StoreCommand FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch)
    {
        _failFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Failed}, exception_json = ?, timestamp = ?, epoch = ?
            WHERE 
                type = ? AND 
                instance = ? AND 
                epoch = ?";

        return StoreCommand.Create(
            _failFunctionSql,
            values: [
                JsonSerializer.Serialize(storedException),
                timestamp,
                expectedEpoch,
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                expectedEpoch,
            ]
        );
    }
    
    private string? _suspendFunctionSql;
    public StoreCommand SuspendFunction(StoredId storedId, long timestamp, int expectedEpoch)
    {
        _suspendFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Suspended}, timestamp = ?
            WHERE type = ? AND 
                  instance = ? AND 
                  epoch = ? AND
                  NOT interrupted";

        return StoreCommand.Create(
            _suspendFunctionSql,
            values: [
                timestamp,
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                expectedEpoch
            ]
        );
    }
    
    private string? _restartExecutionSql;
    public StoreCommand RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration)
    {
        _restartExecutionSql ??= @$"
            UPDATE {tablePrefix}
            SET epoch = epoch + 1, status = {(int)Status.Executing}, expires = ?, interrupted = FALSE
            WHERE type = ? AND instance = ? AND epoch = ?;
            
            SELECT               
                param_json,            
                status,
                result_json, 
                exception_json,
                epoch, 
                expires,
                interrupted,
                timestamp,
                human_instance_id,
                parent,
                owner
            FROM {tablePrefix}
            WHERE type = ? AND instance = ?;";

        var command = StoreCommand.Create(
            _restartExecutionSql,
            values: [
                leaseExpiration,
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                expectedEpoch,
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
            ]);
        return command;
    }
    
    public StoreCommand AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages)
    {
        var sql = @$"    
            INSERT INTO {tablePrefix}_messages
                (type, instance, position, message_json, message_type, idempotency_key)
            VALUES 
                 {"(?, ?, ?, ?, ?, ?)".Replicate(messages.Count).StringJoin($",{Environment.NewLine}")};";

        var command = StoreCommand.Create(sql);
        foreach (var (storedId, (messageContent, messageType, idempotencyKey), position) in messages)
        {
            var (storedType, storedInstance) = storedId;
            command.AddParameter(storedType.Value);
            command.AddParameter(storedInstance.Value.ToString("N"));
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
            WHERE type = ? AND instance = ? AND position >= ?
            ORDER BY position ASC;";

        var command = StoreCommand.Create(
            _getMessagesSql,
            values:
            [
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
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
}