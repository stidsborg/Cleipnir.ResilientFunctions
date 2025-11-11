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
                WHERE Id = ANY($1)";

        return StoreCommand.Create(sql, values: [ storedIds.Select(id => id.AsGuid).ToArray() ]);
    }
    
    private string? _getEffectResultsSql;
    public StoreCommand GetEffects(StoredId storedId)
    {
        _getEffectResultsSql ??= @$"
            SELECT id, content, 0 as position, version
            FROM {tablePrefix}_effects
            WHERE id = $1;";

        return StoreCommand.Create(
            _getEffectResultsSql,
            values: [ storedId.AsGuid ]);
    }
    
    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, content, 0 as position, version
            FROM {tablePrefix}_effects
            WHERE id = ANY($1);";

        return StoreCommand.Create(sql, values: [ storedIds.Select(id => id.AsGuid).ToArray() ]);
    }

    public record StoredEffectsWithSession(IReadOnlyList<StoredEffect> Effects, SnapshotStorageSession Session);
    public async Task<StoredEffectsWithSession> ReadEffects(NpgsqlDataReader reader, ReplicaId replicaId)
    {
        var effects = new List<StoredEffect>();
        var session = new SnapshotStorageSession(replicaId);

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

    public async Task<StoredInputOutput?> ReadStoredFunction(NpgsqlDataReader reader)
    {
        /*
           0  id
           1  param_json
           2  result_json
           3  exception_json
           4  human_instance_id
           5  parent
         */
        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
            var hasParam = !await reader.IsDBNullAsync(1);
            var hasResult = !await reader.IsDBNullAsync(2);
            var hasException = !await reader.IsDBNullAsync(3);
            var hasParent = !await reader.IsDBNullAsync(5);

            var paramJson = hasParam ? (byte[])reader.GetValue(1) : null;
            var resultJson = hasResult ? (byte[])reader.GetValue(2) : null;
            var exceptionJson = hasException ? reader.GetString(3) : null;
            var humanInstanceId = reader.GetString(4);
            var parent = hasParent ? reader.GetGuid(5).ToStoredId() : null;

            return new StoredInputOutput(id, paramJson, resultJson, exceptionJson, humanInstanceId, parent);
        }

        return null;
    }
    public async Task<Dictionary<StoredId, SnapshotStorageSession>> ReadEffectsForIds(NpgsqlDataReader reader, IEnumerable<StoredId> storedIds, ReplicaId owner)
    {
        var effects = new Dictionary<StoredId, SnapshotStorageSession>();
        foreach (var storedId in storedIds)
            effects[storedId] = new SnapshotStorageSession(owner);
        
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
            $@"INSERT INTO {tablePrefix}_effects
                            (id, content, version)
                       VALUES
                            ($1, $2, 0);",
            [storedId.AsGuid, content]
        );
    }
    
    private string? _createFunctionSql;
    private string? _createFunctionWithConflictSql;

    public IEnumerable<StoreCommand> CreateFunction(
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
        if (ignoreConflict)
        {
            _createFunctionWithConflictSql ??= @$"
                INSERT INTO {tablePrefix}
                    (id, status, expires, owner, timestamp, param_json, result_json, exception_json, human_instance_id, parent)
                VALUES
                    ($1, $2, $3, $4, $5, $6, NULL, NULL, $7, $8)
                ON CONFLICT DO NOTHING";

            yield return StoreCommand.Create(
                _createFunctionWithConflictSql,
                values:
                [
                    storedId.AsGuid,
                    (int)(postponeUntil == null ? Status.Executing : Status.Postponed),
                    postponeUntil ?? leaseExpiration,
                    owner?.AsGuid ?? (object)DBNull.Value,
                    timestamp,
                    param == null ? DBNull.Value : param,
                    humanInstanceId.Value,
                    parent?.AsGuid ?? (object)DBNull.Value,
                ]);
        }
        else
        {
            _createFunctionSql ??= @$"
                INSERT INTO {tablePrefix}
                    (id, status, expires, owner, timestamp, param_json, result_json, exception_json, human_instance_id, parent)
                VALUES
                    ($1, $2, $3, $4, $5, $6, NULL, NULL, $7, $8)";

            yield return StoreCommand.Create(
                _createFunctionSql,
                values:
                [
                    storedId.AsGuid,
                    (int)(postponeUntil == null ? Status.Executing : Status.Postponed),
                    postponeUntil ?? leaseExpiration,
                    owner?.AsGuid ?? (object)DBNull.Value,
                    timestamp,
                    param == null ? DBNull.Value : param,
                    humanInstanceId.Value,
                    parent?.AsGuid ?? (object)DBNull.Value,
                ]);
        }
    }
    
    private string? _succeedFunctionSql;
    public IEnumerable<StoreCommand> SucceedFunction(
        StoredId storedId,
        byte[]? result,
        long timestamp,
        ReplicaId expectedReplica)
    {
        _succeedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Succeeded}, owner = NULL, timestamp = $3, result_json = $4
            WHERE id = $1 AND owner = $2";

        yield return StoreCommand.Create(
            _succeedFunctionSql,
            values:
            [
                storedId.AsGuid,
                expectedReplica.AsGuid,
                timestamp,
                result == null ? DBNull.Value : result,
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
                owner = NULL,
                interrupted = FALSE,
                timestamp = $4
            WHERE
                id = $2 AND
                owner = $3;";

        return StoreCommand.Create(
            _postponeFunctionSql,
            values: [
                postponeUntil,
                storedId.AsGuid,
                expectedReplica.AsGuid,
                timestamp,
            ]
        );
    }
    
    private string? _failFunctionSql;
    public IEnumerable<StoreCommand> FailFunction(
        StoredId storedId,
        StoredException storedException,
        long timestamp,
        ReplicaId expectedReplica)
    {
        _failFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Failed}, owner = NULL, timestamp = $3, exception_json = $4
            WHERE id = $1 AND owner = $2";

        yield return StoreCommand.Create(
            _failFunctionSql,
            values:
            [
                storedId.AsGuid,
                expectedReplica.AsGuid,
                timestamp,
                JsonSerializer.Serialize(storedException)
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
                owner = NULL,
                interrupted = FALSE,
                timestamp = $3
            WHERE id = $1 AND owner = $2;";

        return StoreCommand.Create(
            _suspendFunctionSql,
            values: [
                storedId.AsGuid,
                expectedReplica.AsGuid,
                timestamp,
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
                owner;";
/*
 *  0  id
    1  param_json
    2  result_json
    3  exception_json
    4  human_instance_id
    5  parent
 */
        var command = StoreCommand.Create(
            _restartExecutionSql,
            values: [
                replicaId.AsGuid,
                storedId.AsGuid,
            ]);

        return command;
    }

    private string? _restartExecutionsSql;
    public StoreCommand RestartExecutions(IReadOnlyList<StoredId> storedIds, ReplicaId replicaId)
    {
        _restartExecutionsSql ??= @$"
            UPDATE {tablePrefix}
            SET status = {(int)Status.Executing}, expires = 0, interrupted = FALSE, owner = $1
            WHERE id = ANY($2) AND owner IS NULL
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
                owner;";

        return StoreCommand.Create(
            _restartExecutionsSql,
            values: [
                replicaId.AsGuid,
                storedIds.Select(id => id.AsGuid).ToArray()
            ]);
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
            var parent = hasParent ? reader.GetGuid(9).ToStoredId() : null;
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

        foreach (var (storedId, (messageContent, messageType, _, idempotencyKey), position) in messages)
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
            WHERE id = ANY($1);";

        var storeCommand = StoreCommand.Create(sql, values: [ storedIds.Select(id => id.AsGuid).ToArray() ]);
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

    public StoreCommand DeleteMessages(StoredId storedId, IEnumerable<long> positions)
    {
        var positionsArray = positions.ToArray();
        var sql = @$"
                DELETE FROM {tablePrefix}_messages
                WHERE id = $1 AND position = ANY($2)";

        return StoreCommand.Create(sql, values: [ storedId.AsGuid, positionsArray ]);
    }

    private string? _setParametersSql;
    private string? _setParametersSqlWithoutReplica;
    public StoreCommand SetParameters(
        StoredId storedId,
        byte[]? param,
        byte[]? result,
        ReplicaId? expectedReplica)
    {
        if (expectedReplica == null)
        {
            _setParametersSqlWithoutReplica ??= $@"
                UPDATE {tablePrefix}
                SET param_json = $1, result_json = $2
                WHERE id = $3 AND owner IS NULL";

            return StoreCommand.Create(
                _setParametersSqlWithoutReplica,
                values:
                [
                    param ?? (object)DBNull.Value,
                    result ?? (object)DBNull.Value,
                    storedId.AsGuid,
                ]);
        }
        else
        {
            _setParametersSql ??= $@"
                UPDATE {tablePrefix}
                SET param_json = $1, result_json = $2
                WHERE id = $3 AND owner = $4";

            return StoreCommand.Create(
                _setParametersSql,
                values:
                [
                    param ?? (object)DBNull.Value,
                    result ?? (object)DBNull.Value,
                    storedId.AsGuid,
                    expectedReplica.AsGuid,
                ]);
        }
    }

    private string? _bulkScheduleFunctionsSql;
    public IEnumerable<StoreCommand> BulkScheduleFunctions(IdWithParam idWithParam, StoredId? parent)
    {
        _bulkScheduleFunctionsSql ??= @$"
            INSERT INTO {tablePrefix}
                (id, status, expires, timestamp, param_json, result_json, exception_json, human_instance_id, parent)
            VALUES
                ($1, {(int) Status.Postponed}, 0, 0, $2, NULL, NULL, $3, $4)
            ON CONFLICT DO NOTHING";

        yield return StoreCommand.Create(
            _bulkScheduleFunctionsSql,
            values:
            [
                idWithParam.StoredId.AsGuid,
                idWithParam.Param ?? (object)DBNull.Value,
                idWithParam.HumanInstanceId,
                parent?.AsGuid ?? (object)DBNull.Value,
            ]
        );
    }
}