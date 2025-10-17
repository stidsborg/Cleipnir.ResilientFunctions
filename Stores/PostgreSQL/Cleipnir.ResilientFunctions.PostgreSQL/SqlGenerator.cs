using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
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
                WHERE Id IN ({storedIds.Select(id => $"'{id}'").StringJoin(", ")})";
        
        return StoreCommand.Create(sql);
    }
    
    private string? _getEffectResultsSql;
    public StoreCommand GetEffects(StoredId storedId)
    {
        _getEffectResultsSql ??= @$"
            SELECT position, status, result, exception, effect_id
            FROM {tablePrefix}_effects
            WHERE id = $1;";
        
        return StoreCommand.Create(
            _getEffectResultsSql,
            values: [ storedId.AsGuid ]);
    }
    
    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, position, status, result, exception, effect_id
            FROM {tablePrefix}_effects
            WHERE id IN ({storedIds.Select(id => $"'{id}'").StringJoin(", ")});";
        
        return StoreCommand.Create(sql);
    }

    public async Task<IReadOnlyList<StoredEffect>> ReadEffects(NpgsqlDataReader reader)
    {
        var functions = new List<StoredEffect>();
        while (await reader.ReadAsync())
        {
            var position = reader.GetInt64(0);
            var status = (WorkStatus) reader.GetInt32(1);
            var result = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
            var exception = reader.IsDBNull(3) ? null : reader.GetString(3);
            var effectId = reader.GetString(4);
            functions.Add(
                new StoredEffect(EffectId.Deserialize(effectId), status, result, JsonHelper.FromJson<StoredException>(exception))
            );
        }

        return functions;
    }
    public async Task<Dictionary<StoredId, List<StoredEffect>>> ReadEffectsForIds(NpgsqlDataReader reader, IEnumerable<StoredId> storedIds)
    {
        var effects = new Dictionary<StoredId, List<StoredEffect>>();
        foreach (var storedId in storedIds)
            effects[storedId] = new List<StoredEffect>();
        
        while (await reader.ReadAsync())
        {
            var id = new StoredId(reader.GetGuid(0));
            var position = reader.GetInt64(1);
            var status = (WorkStatus) reader.GetInt32(2);
            var result = reader.IsDBNull(3) ? null : (byte[]) reader.GetValue(3);
            var exception = reader.IsDBNull(4) ? null : reader.GetString(4);
            var effectId = reader.GetString(5);

            var se = new StoredEffect(EffectId.Deserialize(effectId), status, result, JsonHelper.FromJson<StoredException>(exception));
            effects[id].Add(se);
        }

        return effects;
    }
    
    public IEnumerable<StoreCommand> UpdateEffects(IReadOnlyList<StoredEffectChange> changes)
   {
       var commands = new List<StoreCommand>(changes.Count);

       // INSERT
       {
           var sql= $@"
                INSERT INTO {tablePrefix}_effects
                    (id, position, status, result, exception, effect_id)
                VALUES
                    ($1, $2, $3, $4, $5, $6);";
      
           foreach (var (storedId, _, _, storedEffect) in changes.Where(s => s.Operation == CrudOperation.Insert))
           {
               var command = StoreCommand.Create(sql);
               command.AddParameter(storedId.AsGuid);
               command.AddParameter(storedEffect!.StoredEffectId.Value.ToLong());
               command.AddParameter((int) storedEffect.WorkStatus);
               command.AddParameter(storedEffect.Result ?? (object) DBNull.Value);
               command.AddParameter(JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value);
               command.AddParameter(storedEffect.EffectId.Serialize());
           
               commands.Add(command);
           }   
       }
       
       // UPDATE
       {
            var sql= $@"
                UPDATE {tablePrefix}_effects
                SET status = $1, result = $2, exception = $3
                WHERE id = $4 AND position = $5;";
      
           foreach (var (storedId, _, _, storedEffect) in changes.Where(s => s.Operation == CrudOperation.Update))
           {
               var command = StoreCommand.Create(sql);
               command.AddParameter((int) storedEffect!.WorkStatus);
               command.AddParameter(storedEffect.Result ?? (object) DBNull.Value);
               command.AddParameter(JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value);
               command.AddParameter(storedId.AsGuid);
               command.AddParameter(storedEffect.StoredEffectId.Value.ToLong());
           
               commands.Add(command);
           }   
       }

       // DELETE
       var removedEffects = changes
           .Where(s => s.Operation == CrudOperation.Delete)
           .Select(s => new { Id = s.StoredId, s.EffectId })
           .GroupBy(s => s.Id, s => s.EffectId.ToStoredEffectId().Value.ToLong());

       foreach (var removedEffectGroup in removedEffects)
       {
           var storedId = removedEffectGroup.Key;
           var removeSql = @$"
                DELETE FROM {tablePrefix}_effects
                WHERE id = '{storedId.AsGuid}' AND
                      position IN ({removedEffectGroup.Select(id => $"'{id}'").StringJoin(", ")});";
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
        ReplicaId? owner,
        bool ignoreConflict)
    {
        _createFunctionSql ??= @$"
            INSERT INTO {tablePrefix}
                (id, status, param_json, expires, timestamp, human_instance_id, parent, owner)
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
                storedId.AsGuid,
                (int)(postponeUntil == null ? Status.Executing : Status.Postponed),
                param == null ? DBNull.Value : param,
                postponeUntil ?? leaseExpiration,
                timestamp,
                humanInstanceId.Value,
                parent?.Serialize() ?? (object)DBNull.Value,
                owner?.AsGuid ?? (object)DBNull.Value,
            ]);
    }
    
    private string? _succeedFunctionSql;
    public StoreCommand SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        ReplicaId expectedReplica)
    {
        _succeedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Succeeded}, result_json = $1, timestamp = $2, owner = NULL
            WHERE id = $3 AND owner = $4";

        return StoreCommand.Create(
            _succeedFunctionSql,
            values:
            [
                result == null ? DBNull.Value : result,
                timestamp,
                storedId.AsGuid,
                expectedReplica.AsGuid,
            ]
        );
    }
    
    private string? _postponeFunctionSql;
    public StoreCommand PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        ReplicaId expectedReplica)
    {
        _postponeFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Postponed},
                expires = CASE WHEN interrupted THEN 0 ELSE $1 END,
                timestamp = $2,
                owner = NULL,
                interrupted = FALSE
            WHERE
                id = $3 AND
                owner = $4";

        return StoreCommand.Create(
            _postponeFunctionSql,
            values: [
                postponeUntil,
                timestamp,
                storedId.AsGuid,
                expectedReplica.AsGuid,
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
            SET status = {(int) Status.Failed}, exception_json = $1, timestamp = $2, owner = NULL
            WHERE id = $3 AND owner = $4";
        return StoreCommand.Create(
            _failFunctionSql,
            values:
            [
                JsonSerializer.Serialize(storedException),
                timestamp,
                storedId.AsGuid,
                expectedReplica.AsGuid,
            ]
        );
    }
    
    private string? _suspendFunctionSql;
    public StoreCommand SuspendFunction(StoredId storedId, long timestamp, ReplicaId expectedReplica)
    {
        _suspendFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = CASE WHEN interrupted THEN {(int) Status.Postponed} ELSE {(int) Status.Suspended} END,
                expires = 0,
                timestamp = $1,
                owner = NULL,
                interrupted = FALSE
            WHERE id = $2 AND owner = $3";

        return StoreCommand.Create(
            _suspendFunctionSql,
            values: [
                timestamp,
                storedId.AsGuid,
                expectedReplica.AsGuid,
            ]
        );
    }

    private string? _restartExecutionSql;
    public StoreCommand RestartExecution(StoredId storedId, ReplicaId replicaId)
    {
        _restartExecutionSql ??= @$"
            UPDATE {tablePrefix}
            SET status = {(int)Status.Executing}, expires = 0, interrupted = FALSE, owner = $1
            WHERE id = $2 AND owner IS NULL
            RETURNING         
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
                owner";

        var command = StoreCommand.Create(
            _restartExecutionSql,
            values: [
                replicaId.AsGuid,
                storedId.AsGuid,
            ]);

        return command;
    }
    
    public async Task<StoredFlow?> ReadFunction(StoredId storedId, NpgsqlDataReader reader)
    {
        /*
           0  id
           1  param_json,
           2  status,
           3  result_json,
           4  exception_json,
           5  expires,
           6 interrupted,
           7 timestamp,
           8 human_instance_id
           9 parent,
           10 owner
         */
        while (await reader.ReadAsync())
        {
            var hasParameter = !await reader.IsDBNullAsync(1);
            var hasResult = !await reader.IsDBNullAsync(3);
            var hasException = !await reader.IsDBNullAsync(4);
            var hasParent = !await reader.IsDBNullAsync(9);
            var hasOwner = !await reader.IsDBNullAsync(10);
            
            var id = reader.GetGuid(0).ToStoredId();
            var param = hasParameter ? (byte[]) reader.GetValue(1) : null;
            var status = (Status) reader.GetInt32(2);
            var result = hasResult ? (byte[]) reader.GetValue(3) : null;
            var exception = hasException ? JsonSerializer.Deserialize<StoredException>(reader.GetString(4)) : null;
            var expires = reader.GetInt64(5);
            var interrupted = reader.GetBoolean(6);
            var timestamp = reader.GetInt64(7);
            var humanInstanceId = reader.GetString(8);
            var parent = hasParent ? StoredId.Deserialize(reader.GetString(9)) : null;
            var owner = hasOwner ? new ReplicaId(reader.GetGuid(10)) : null;
            
            return new StoredFlow(
                id,
                humanInstanceId,
                param,
                status,
                result, 
                exception,
                expires,
                timestamp,
                interrupted,
                parent,
                owner,
                id.Type
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
                 {messages.Select((_, i) => $"(${i * 5 + 1}, ${i * 5 + 2}, ${i * 5 + 3}, ${i * 5 + 4}, ${i * 5 + 5})").StringJoin($",{Environment.NewLine}")};";

        var command = StoreCommand.Create(sql);

        foreach (var (storedId, (messageContent, messageType, idempotencyKey), position) in messages)
        {
            command.AddParameter(storedId.AsGuid);
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
            WHERE id = $1 AND position >= $2
            ORDER BY position ASC;";

        var storeCommand = StoreCommand.Create(
            _getMessagesSql,
            values: [storedId.AsGuid, skip]
        );
        
        return storeCommand;
    }

    public async Task<IReadOnlyList<StoredMessage>> ReadMessages(NpgsqlDataReader reader)
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
            WHERE id IN ({storedIds.InClause()});";

        var storeCommand = StoreCommand.Create(sql);
        return storeCommand;
    }
    
    public async Task<Dictionary<StoredId, List<StoredMessage>>> ReadStoredIdsMessages(NpgsqlDataReader reader)
    {
        var messages = new Dictionary<StoredId, List<StoredMessageWithPosition>>();
        
        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
            var position = reader.GetInt32(1);
            var messageJson = (byte[]) reader.GetValue(2);
            var messageType = (byte[]) reader.GetValue(3);
            var idempotencyKey = reader.IsDBNull(4) ? null : reader.GetString(4);

            if (!messages.ContainsKey(id))
                messages[id] = new List<StoredMessageWithPosition>();

            var storedMessage = new StoredMessage(messageJson, messageType, idempotencyKey);
            messages[id].Add(new StoredMessageWithPosition(storedMessage, position));
        }

        return messages.ToDictionary(kv => kv.Key, kv => kv.Value.OrderBy(m => m.Position).Select(m => m.StoredMessage).ToList());
    }
}