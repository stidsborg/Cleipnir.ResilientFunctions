using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class SqlGenerator(string tablePrefix)
{
    public StoreCommand Interrupt(IEnumerable<StoredId> storedIds)
    {
        var conditionals = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(group => $"(type = {group.Key} AND instance IN ({group.Select(i => $"'{i}'").StringJoin(", ")}))")
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
                WHERE {conditionals}";
        
        return StoreCommand.Create(sql);
    }
    
    public IEnumerable<StoreCommand> UpdateEffects(IReadOnlyList<StoredEffectChange> changes)
   {
       var commands = new List<StoreCommand>(changes.Count);
       var sql= $@"
         INSERT INTO {tablePrefix}_effects
             (type, instance, id_hash, status, result, exception, effect_id)
         VALUES
             ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (type, instance, id_hash)
         DO
           UPDATE SET status = EXCLUDED.status, result = EXCLUDED.result, exception = EXCLUDED.exception";
      
       foreach (var (storedId, _, _, storedEffect) in changes.Where(s => s.Operation == CrudOperation.Upsert))
       {
           var command = StoreCommand.Create(sql);
           command.AddParameter(storedId.Type.Value);
           command.AddParameter(storedId.Instance.Value);
           command.AddParameter(storedEffect!.StoredEffectId.Value);
           command.AddParameter((int) storedEffect.WorkStatus);
           command.AddParameter(storedEffect.Result ?? (object) DBNull.Value);
           command.AddParameter(JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value);
           command.AddParameter(storedEffect.EffectId.Serialize());
           
           commands.Add(command);
       }

       var removedEffects = changes
           .Where(s => s.Operation == CrudOperation.Delete)
           .Select(s => new { Id = s.StoredId, s.EffectId })
           .GroupBy(s => s.Id, s => s.EffectId.Value);

       foreach (var removedEffectGroup in removedEffects)
       {
           var storedId = removedEffectGroup.Key;
           var removeSql = @$"
                DELETE FROM {tablePrefix}_effects
                WHERE type = {storedId.Type.Value} AND
                      instance = '{storedId.Instance.Value}' AND
                      id_hash IN ({removedEffectGroup.Select(id => $"'{id}'").StringJoin(", ")});";
           var command = StoreCommand.Create(removeSql);
           commands.Add(command);
       }

       return commands;
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
        bool ignoreConflict)
    {
        _createFunctionSql ??= @$"
            INSERT INTO {tablePrefix}
                (type, instance, status, param_json, expires, timestamp, human_instance_id, parent)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT DO NOTHING;";

        var sql = _createFunctionSql;
        if (!ignoreConflict)
            sql = sql.Replace("ON CONFLICT DO NOTHING", "");

        return StoreCommand.Create(
            sql,
            values:
            [
                storedId.Type.Value,
                storedId.Instance.Value,
                (int)(postponeUntil == null ? Status.Executing : Status.Postponed),
                param == null ? DBNull.Value : param,
                postponeUntil ?? leaseExpiration,
                timestamp,
                humanInstanceId.Value,
                parent?.Serialize() ?? (object)DBNull.Value
            ]);
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
            SET status = {(int) Status.Succeeded}, result_json = $1, timestamp = $2
            WHERE 
                type = $3 AND 
                instance = $4 AND 
                epoch = $5";

        return StoreCommand.Create(
            _succeedFunctionSql,
            values:
            [
                result == null ? DBNull.Value : result,
                timestamp,
                storedId.Type.Value,
                storedId.Instance.Value,
                expectedEpoch,
            ]
        );
    }
    
    private string? _postponeFunctionSql;
    public StoreCommand PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        bool ignoreInterrupted,
        int expectedEpoch)
    {
        _postponeFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Postponed}, expires = $1, timestamp = $2
            WHERE 
                type = $3 AND 
                instance = $4 AND 
                epoch = $5 AND
                interrupted = FALSE";
        
        var sql = _postponeFunctionSql;
        if (ignoreInterrupted)
            sql = sql.Replace("interrupted = FALSE", "1 = 1");

        return StoreCommand.Create(
            sql,
            values: [
                postponeUntil,
                timestamp,
                storedId.Type.Value,
                storedId.Instance.Value,
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
            SET status = {(int) Status.Failed}, exception_json = $1, timestamp = $2
            WHERE 
                type = $3 AND 
                instance = $4 AND 
                epoch = $5";
        return StoreCommand.Create(
            _failFunctionSql,
            values:
            [
                JsonSerializer.Serialize(storedException),
                timestamp,
                storedId.Type.Value,
                storedId.Instance.Value,
                expectedEpoch,
            ]
        );
    }
    
    private string? _suspendFunctionSql;
    public StoreCommand SuspendFunction(StoredId storedId, long timestamp, int expectedEpoch)
    {
        _suspendFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int)Status.Suspended}, timestamp = $1
            WHERE type = $2 AND 
                  instance = $3 AND 
                  epoch = $4 AND
                  NOT interrupted;";

        return StoreCommand.Create(
            _suspendFunctionSql,
            values: [
                timestamp,
                storedId.Type.Value,
                storedId.Instance.Value,
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
                 {messages.Select((_, i) => $"(${i * 6 + 1}, ${i * 6 + 2}, ${i * 6 + 3}, ${i * 6 + 4}, ${i * 6 + 5}, ${i * 6 + 6})").StringJoin($",{Environment.NewLine}")};";

        var command = StoreCommand.Create(sql);

        foreach (var (storedId, (messageContent, messageType, idempotencyKey), position) in messages)
        {
            var (storedType, storedInstance) = storedId;
            
            command.AddParameter(storedType.Value);
            command.AddParameter(storedInstance.Value);
            command.AddParameter(position);
            command.AddParameter(messageContent);
            command.AddParameter(messageType);
            command.AddParameter(idempotencyKey ?? (object)DBNull.Value);
        }

        return command;
    }
}