using System.Text;
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
    public StoreCommand? Interrupt(IEnumerable<StoredId> storedIds)
    {
        var conditionals = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(group => $"(type = {group.Key} AND instance IN ({group.Select(i => $"'{i:N}'").StringJoin(", ")}))")
            .StringJoin(" OR ");
        if (string.IsNullOrEmpty(conditionals))
            return null;

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

        return new StoreCommand(sql);
    }
    
    public StoreCommand? UpdateEffects(IReadOnlyList<StoredEffectChange> changes)
    {
        if (!changes.Any())
            return null;

        var upsertCommand = default(StoreCommand);
        if (changes.Any(c => c.Operation == CrudOperation.Upsert))
        {
            var upserts = changes
                .Where(c => c.Operation == CrudOperation.Upsert)
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

            upsertCommand = new StoreCommand(setSql);
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
            
            removeCommand = new StoreCommand(removeSql);
        }
        
        return StoreCommand.Merge(upsertCommand, removeCommand);
    }
    
    private string? _createFunctionSql;
    public StoreCommand CreateFunction(
        StoredId storedId, 
        FlowInstance humanInstanceId,
        byte[]? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent)
    {
        _createFunctionSql ??= @$"
            INSERT IGNORE INTO {tablePrefix}
                (type, instance, param_json, status, epoch, expires, timestamp, human_instance_id, parent)
            VALUES
                (?, ?, ?, ?, 0, ?, ?, ?, ?)";
        var status = postponeUntil == null ? Status.Executing : Status.Postponed;

        return new StoreCommand(
            _createFunctionSql,
            [
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                param ?? (object)DBNull.Value,
                (int)status,
                postponeUntil ?? leaseExpiration,
                timestamp,
                humanInstanceId.Value,
                parent?.Serialize() ?? (object)DBNull.Value,
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

        return new StoreCommand(
            _succeedFunctionSql,
            [
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

        return new StoreCommand(
            sql,
            [
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

        return new StoreCommand(
            _failFunctionSql,
            [
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

        return new StoreCommand(
            _suspendFunctionSql,
            [
                timestamp,
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                expectedEpoch
            ]
        );
    }

    public StoreCommand AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages)
    {
        var sql = @$"    
            INSERT INTO {tablePrefix}_messages
                (type, instance, position, message_json, message_type, idempotency_key)
            VALUES 
                 {"(?, ?, ?, ?, ?, ?)".Replicate(messages.Count).StringJoin($",{Environment.NewLine}")};";

        var command = new StoreCommand(sql);
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
}