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
    public StoreCommand InsertEffects(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, SnapshotStorageSession session)
    {
        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                session.Effects.Remove(change.EffectId);
            else
                session.Effects[change.EffectId] = change.StoredEffect!;

        var content = session.Serialize();
        session.RowExists = true;
        // Runs in the CreateFunction transaction right after the flow row's INSERT - no owner guard needed.
        return StoreCommand.Create(
            $"UPDATE {tablePrefix} SET effects = $1 WHERE id = $2;",
            [content, storedId.AsGuid]
        );
    }
    
    private string? _createFunctionSql;
    private string? _createFunctionWithConflictSql;

    public IEnumerable<StoreCommand> CreateFunction(
        StoredId storedId,
        FlowInstance humanInstanceId,
        byte[]? param,
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
                    postponeUntil ?? 0,
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
                    postponeUntil ?? 0,
                    owner?.AsGuid ?? (object)DBNull.Value,
                    timestamp,
                    param == null ? DBNull.Value : param,
                    humanInstanceId.Value,
                    parent?.AsGuid ?? (object)DBNull.Value,
                ]);
        }
    }
    
    private string? _succeedFunctionSql;
    private string? _succeedFunctionWithEffectsSql;
    public IEnumerable<StoreCommand> SucceedFunction(
        StoredId storedId,
        byte[]? result,
        long timestamp,
        ReplicaId expectedReplica,
        byte[]? effects = null)
    {
        if (effects == null)
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
        else
        {
            _succeedFunctionWithEffectsSql ??= $@"
                UPDATE {tablePrefix}
                SET status = {(int) Status.Succeeded}, owner = NULL, timestamp = $3, result_json = $4, effects = $5
                WHERE id = $1 AND owner = $2";

            yield return StoreCommand.Create(
                _succeedFunctionWithEffectsSql,
                values:
                [
                    storedId.AsGuid,
                    expectedReplica.AsGuid,
                    timestamp,
                    result == null ? DBNull.Value : result,
                    effects,
                ]
            );
        }
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
                expires = $1,
                owner = NULL,
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
    private string? _failFunctionWithEffectsSql;
    public IEnumerable<StoreCommand> FailFunction(
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
        else
        {
            _failFunctionWithEffectsSql ??= $@"
                UPDATE {tablePrefix}
                SET status = {(int) Status.Failed}, owner = NULL, timestamp = $3, exception_json = $4, effects = $5
                WHERE id = $1 AND owner = $2";

            yield return StoreCommand.Create(
                _failFunctionWithEffectsSql,
                values:
                [
                    storedId.AsGuid,
                    expectedReplica.AsGuid,
                    timestamp,
                    JsonSerializer.Serialize(storedException),
                    effects,
                ]
            );
        }
    }
    
    private string? _suspendFunctionSql;
    public StoreCommand SuspendFunction(StoredId storedId, long timestamp, ReplicaId expectedReplica)
    {
        _suspendFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Suspended},
                expires = 0,
                owner = NULL,
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
            SET status = {(int)Status.Executing}, expires = 0, owner = $1
            WHERE id = $2 AND owner IS NULL
            RETURNING
                id,
                param_json,
                status,
                result_json,
                exception_json,
                expires,
                timestamp,
                human_instance_id,
                parent,
                owner,
                effects;";
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
        // Restartable flows are the parked ones (postponed/suspended): the batch restart backs the watchdogs, which
        // must never resurrect a completed flow - e.g. when a message arrives after its target has succeeded.
        _restartExecutionsSql ??= @$"
            UPDATE {tablePrefix}
            SET status = {(int)Status.Executing}, expires = 0, owner = $1
            WHERE id = ANY($2) AND owner IS NULL AND status IN ({(int)Status.Postponed}, {(int)Status.Suspended})
            RETURNING
                id,
                param_json,
                status,
                result_json,
                exception_json,
                expires,
                timestamp,
                human_instance_id,
                parent,
                owner,
                effects;";

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
           6 timestamp,
           7 human_instance_id
           8 parent,
           9 owner
         */
        while (await reader.ReadAsync())
        {
            var hasParameter = !await reader.IsDBNullAsync(1);
            var hasResult = !await reader.IsDBNullAsync(3);
            var hasException = !await reader.IsDBNullAsync(4);
            var hasParent = !await reader.IsDBNullAsync(8);
            var hasOwner = !await reader.IsDBNullAsync(9);

            var id = reader.GetGuid(0).ToStoredId();
            var param = hasParameter ? (byte[]) reader.GetValue(1) : null;
            var status = (Status) reader.GetInt32(2);
            var result = hasResult ? (byte[]) reader.GetValue(3) : null;
            var exception = hasException ? JsonSerializer.Deserialize<StoredException>(reader.GetString(4)) : null;
            var expires = reader.GetInt64(5);
            var timestamp = reader.GetInt64(6);
            var humanInstanceId = reader.GetString(7);
            var parent = hasParent ? reader.GetGuid(8).ToStoredId() : null;
            var owner = hasOwner ? new ReplicaId(reader.GetGuid(9)) : null;

            return new StoredFlow(
                id,
                humanInstanceId,
                param,
                status,
                exception,
                expires,
                timestamp,
                parent,
                owner,
                id.Type
            );
        }

        return null;
    } 

    private string? _appendMessagesSql;
    public StoreCommand AppendMessages(StoredId storedId, IEnumerable<StoredMessage> messages)
    {
        // position is assigned by the table's identity column. unnest($2::bytea[], $3::uuid[]) WITH ORDINALITY
        // expands the content/fallback-replica parameters into rows in parallel; ORDER BY ord makes the identity
        // assignment follow caller order. replica is the target flow's current owner, falling back to the
        // publisher's replica when the target flow is not executing.
        _appendMessagesSql ??= @$"
            INSERT INTO {tablePrefix}_messages (id, replica, content)
            SELECT $1, COALESCE((SELECT owner FROM {tablePrefix} WHERE id = $1), replica), content
            FROM unnest($2::bytea[], $3::uuid[]) WITH ORDINALITY AS t(content, replica, ord)
            ORDER BY ord;";

        var materialized = messages.ToList();
        var contents = materialized
            .Select(m => BinaryPacker.Pack(
                m.MessageContent,
                m.MessageType,
                m.IdempotencyKey?.ToUtf8Bytes(),
                m.Sender?.ToUtf8Bytes(),
                m.Receiver?.ToUtf8Bytes()
            ))
            .ToArray();
        var replicas = materialized
            .Select(m => m.Replica.AsGuid)
            .ToArray();

        return StoreCommand.Create(
            _appendMessagesSql,
            values: [
                storedId.AsGuid,
                contents,
                replicas
            ]
        );
    }

    // The identity column assigns position. Rows are listed in caller order so identity assignment preserves
    // message order.
    public StoreCommand AppendMessages(IReadOnlyList<StoredIdAndMessage> messages)
    {
        // replica is each message's target flow owner, falling back to the publisher's replica when the target
        // flow is not executing. The ordinal column preserves caller order so the identity column assigns
        // positions accordingly.
        var rows = messages
            .Select((_, i) => i == 0
                ? $"($1::uuid, $2::uuid, $3::bytea, {i})"
                : $"(${i * 3 + 1}, ${i * 3 + 2}, ${i * 3 + 3}, {i})")
            .StringJoin($",{Environment.NewLine}");
        var sql = @$"
            INSERT INTO {tablePrefix}_messages (id, replica, content)
            SELECT v.id, COALESCE((SELECT owner FROM {tablePrefix} WHERE id = v.id), v.replica), v.content
            FROM (VALUES {rows}) AS v(id, replica, content, ord)
            ORDER BY v.ord;";

        var command = StoreCommand.Create(sql);

        foreach (var (storedId, (messageContent, messageType, _, replica, idempotencyKey, sender, receiver)) in messages)
        {
            command.AddParameter(storedId.AsGuid);
            command.AddParameter(replica.AsGuid);
            var content = BinaryPacker.Pack(
                messageContent,
                messageType,
                idempotencyKey?.ToUtf8Bytes(),
                sender?.ToUtf8Bytes(),
                receiver?.ToUtf8Bytes()
            );
            command.AddParameter(content);
        }

        return command;
    }
    
    private string? _getMessagesSql;
    public StoreCommand GetMessages(StoredId storedId)
    {
        _getMessagesSql ??= @$"
            SELECT content, position, replica
            FROM {tablePrefix}_messages
            WHERE id = $1
            ORDER BY position ASC;";

        var storeCommand = StoreCommand.Create(
            _getMessagesSql,
            values: [storedId.AsGuid]
        );

        return storeCommand;
    }

    public StoreCommand GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
    {
        var sql = @$"
            SELECT content, position, replica
            FROM {tablePrefix}_messages
            WHERE id = $1 AND position != ALL($2)
            ORDER BY position;";

        var storeCommand = StoreCommand.Create(
            sql,
            values: [storedId.AsGuid, skipPositions.ToArray()]
        );

        return storeCommand;
    }

    public async Task<IReadOnlyList<(byte[] content, long position, Guid? replica)>> ReadMessages(NpgsqlDataReader reader)
    {
        var messages = new List<(byte[], long, Guid?)>();
        while (await reader.ReadAsync())
        {
            var content = (byte[]) reader.GetValue(0);
            var position = reader.GetInt64(1);
            var replica = await reader.IsDBNullAsync(2) ? (Guid?) null : reader.GetGuid(2);
            messages.Add((content, position, replica));
        }

        return messages;
    }

    public StoreCommand GetMessages(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT id, position, content, replica
            FROM {tablePrefix}_messages
            WHERE id = ANY($1)
            ORDER BY position;";

        var storeCommand = StoreCommand.Create(sql, values: [ storedIds.Select(id => id.AsGuid).ToArray() ]);
        return storeCommand;
    }

    public StoreCommand GetMessagesForReplica(ReplicaId replicaId, IReadOnlyList<long> ignorePositions)
    {
        var sql = @$"
            SELECT id, position, content, replica
            FROM {tablePrefix}_messages
            WHERE replica = $1 AND position != ALL($2)
            ORDER BY position;";

        return StoreCommand.Create(sql, values: [ replicaId.AsGuid, ignorePositions.ToArray() ]);
    }

    public StoreCommand GetCrashedReplicaMessages(IReadOnlySet<ReplicaId> liveReplicas)
    {
        var sql = @$"
            SELECT id, position
            FROM {tablePrefix}_messages
            WHERE replica != ALL($1)";

        return StoreCommand.Create(sql, values: [ liveReplicas.Select(r => r.AsGuid).ToArray() ]);
    }

    public async Task<List<StoredIdAndPosition>> ReadStoredIdAndPositions(NpgsqlDataReader reader)
    {
        var result = new List<StoredIdAndPosition>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
            var position = reader.GetInt64(1);
            result.Add(new StoredIdAndPosition(id, position));
        }

        return result;
    }

    public StoreCommand SetReplica(IEnumerable<long> positions, ReplicaId newReplica, ReplicaId expectedReplica)
    {
        var sql = @$"
            UPDATE {tablePrefix}_messages
            SET replica = $1
            WHERE position = ANY($2) AND replica = $3";

        return StoreCommand.Create(
            sql,
            values: [ newReplica.AsGuid, positions.ToArray(), expectedReplica.AsGuid ]
        );
    }
    
    public async Task<Dictionary<StoredId, List<(byte[] content, long position, Guid? replica)>>> ReadStoredIdsMessages(NpgsqlDataReader reader)
    {
        var messages = new Dictionary<StoredId, List<(byte[] content, long position, Guid? replica)>>();

        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
            var position = reader.GetInt64(1);
            var content = (byte[]) reader.GetValue(2);
            var replica = await reader.IsDBNullAsync(3) ? (Guid?) null : reader.GetGuid(3);

            if (!messages.ContainsKey(id))
                messages[id] = new List<(byte[], long, Guid?)>();

            messages[id].Add((content, position, replica));
        }

        return messages.ToDictionary(
            kv => kv.Key,
            kv => kv.Value
                .OrderBy(m => m.position)
                .ToList());
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