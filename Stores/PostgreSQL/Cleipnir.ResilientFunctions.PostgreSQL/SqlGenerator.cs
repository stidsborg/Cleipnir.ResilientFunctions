using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
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
            SELECT id, content, position, version
            FROM {tablePrefix}_state
            WHERE id = $1 AND position = 0;";

        return StoreCommand.Create(
            _getEffectResultsSql,
            values: [ storedId.AsGuid ]);
    }
    
    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, position, content, version
            FROM {tablePrefix}_effects
            WHERE id IN ({storedIds.Select(id => $"'{id}'").StringJoin(", ")});";
        
        return StoreCommand.Create(sql);
    }

    public record StoredEffectsWithSession(IReadOnlyList<StoredEffect> Effects, SnapshotStorageSession Session);
    public async Task<StoredEffectsWithSession> ReadEffects(NpgsqlDataReader reader)
    {
        var effects = new List<StoredEffect>();
        var session = new SnapshotStorageSession();
        
        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0);
            var content = (byte[])reader.GetValue(1);
            var position = reader.GetInt32(2);
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
    public async Task<Dictionary<StoredId, SnapshotStorageSession>> ReadEffectsForIds(NpgsqlDataReader reader, IEnumerable<StoredId> storedIds)
    {
        var effects = new Dictionary<StoredId, SnapshotStorageSession>();
        foreach (var storedId in storedIds)
            effects[storedId] = new SnapshotStorageSession();
        
        while (await reader.ReadAsync())
        {
            var id = new StoredId(reader.GetGuid(0));
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
                            ($1, 0, $2, 0);", 
            [storedId.AsGuid, content]
        );
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
                (id, position, content)
            VALUES 
                 {messages.Select((_, i) => $"(${i * 3 + 1}, ${i * 3 + 2}, ${i * 3 + 3})").StringJoin($",{Environment.NewLine}")};";

        var command = StoreCommand.Create(sql);

        foreach (var (storedId, (messageContent, messageType, idempotencyKey), position) in messages)
        {
            command.AddParameter(storedId.AsGuid);
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
            WHERE id = $1 AND position >= $2
            ORDER BY position ASC;";

        var storeCommand = StoreCommand.Create(
            _getMessagesSql,
            values: [storedId.AsGuid, skip]
        );
        
        return storeCommand;
    }
    
    public async Task<IReadOnlyList<(byte[] content, long position)>> ReadMessages(NpgsqlDataReader reader)
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
            WHERE id IN ({storedIds.InClause()});";

        var storeCommand = StoreCommand.Create(sql);
        return storeCommand;
    }
    
    public async Task<Dictionary<StoredId, List<(byte[] content, long position)>>> ReadStoredIdsMessages(NpgsqlDataReader reader)
    {
        var messages = new Dictionary<StoredId, List<(byte[] content, long position)>>();

        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
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