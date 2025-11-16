using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
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
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlGenerator(string tablePrefix)
{
    public StoreCommand Interrupt(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
                UPDATE {tablePrefix}
                SET 
                    Interrupted = 1,
                    Status = 
                        CASE 
                            WHEN Status = {(int)Status.Suspended} THEN {(int)Status.Postponed}
                            ELSE Status
                        END,
                    Expires = 
                        CASE
                            WHEN Status = {(int)Status.Postponed} THEN 0
                            WHEN Status = {(int)Status.Suspended} THEN 0
                            ELSE Expires
                        END
                WHERE Id IN ({storedIds.Select(id => $"'{id.AsGuid}'").StringJoin(", ")});";

        return StoreCommand.Create(sql);
    }
    
    public StoreCommand InsertEffects(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, SnapshotStorageSession session, string paramPrefix)
    {
        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                session.Effects.Remove(change.EffectId);
            else
                session.Effects[change.EffectId] = change.StoredEffect!;

        var content = session.Serialize();
        session.RowExists = true;
        var updateSql = $@"UPDATE {tablePrefix}
                       SET Effects = @{paramPrefix}Content
                       WHERE Id = @{paramPrefix}Id;";
        var updateCommand = StoreCommand.Create(updateSql);
        updateCommand.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
        updateCommand.AddParameter($"@{paramPrefix}Content", content);
        return updateCommand;
    }
    
    private string? _getEffectsSql;
    public StoreCommand GetEffects(StoredId storedId, string paramPrefix = "")
    {
        _getEffectsSql ??= @$"
            SELECT Id, Effects
            FROM {tablePrefix}
            WHERE Id = @Id";

        var sql = _getEffectsSql;
        if (paramPrefix != "")
            sql = sql.Replace("@", $"@{paramPrefix}");

        var command = StoreCommand.Create(sql);
        command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
        return command;
    }

    public record StoredEffectsWithSession(IReadOnlyList<StoredEffect> Effects, SnapshotStorageSession Session);
    public async Task<StoredEffectsWithSession> ReadEffects(SqlDataReader reader, ReplicaId replicaId)
    {
        var effects = new List<StoredEffect>();
        var session = new SnapshotStorageSession(replicaId);

        while (reader.HasRows && await reader.ReadAsync())
        {
            var id = reader.GetGuid(0);
            var hasEffects = !await reader.IsDBNullAsync(1);

            if (hasEffects)
            {
                var content = (byte[])reader.GetValue(1);
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
                session.Version = 0;
            }
        }

        return new StoredEffectsWithSession(effects, session);
    }
    
    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT Id, Effects
            FROM {tablePrefix}
            WHERE Id IN ({storedIds.InClause()})";

        var command = StoreCommand.Create(sql);
        return command;
    }

    public async Task<Dictionary<StoredId, SnapshotStorageSession>> ReadEffectsForMultipleStoredIds(SqlDataReader reader, IEnumerable<StoredId> storedIds, ReplicaId ownerId)
    {
        var effects = new Dictionary<StoredId, SnapshotStorageSession>();
        foreach (var storedId in storedIds)
            effects[storedId] = new SnapshotStorageSession(ownerId);

        while (reader.HasRows && await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
            var hasEffects = !await reader.IsDBNullAsync(1);

            if (hasEffects)
            {
                var content = (byte[])reader.GetValue(1);
                var effectsBytes = BinaryPacker.Split(content);
                var storedEffects = effectsBytes.Select(effectBytes => StoredEffect.Deserialize(effectBytes!)).ToList();

                var session = effects[id];
                foreach (var storedEffect in storedEffects)
                    session.Effects[storedEffect.EffectId] = storedEffect;

                session.RowExists = true;
                session.Version = 0;
            }
        }

        return effects;
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
        string? paramPrefix,
        byte[]? effects)
    {
        _createFunctionSql ??= @$"
                    INSERT INTO {tablePrefix}(
                        Id,
                        ParamJson,
                        Status,
                        Expires,
                        Timestamp,
                        HumanInstanceId,
                        Parent,
                        Owner,
                        Effects                                                              
                    )
                    VALUES
                    (
                        @Id,
                        @ParamJson,
                        @Status,
                        @Expires,
                        @Timestamp,
                        @HumanInstanceId,
                        @Parent,
                        @Owner,
                        @Effects
                    )";

        var sql = _createFunctionSql;
        if (paramPrefix != null)
            sql = sql.Replace("@", $"@{paramPrefix}");

        var command = StoreCommand.Create(sql);
        command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
        command.AddParameter($"@{paramPrefix}Status", (int)(postponeUntil == null ? Status.Executing : Status.Postponed));
        command.AddParameter($"@{paramPrefix}ParamJson", param == null ? SqlBinary.Null : param);
        command.AddParameter($"@{paramPrefix}Expires", postponeUntil ?? leaseExpiration);
        command.AddParameter($"@{paramPrefix}HumanInstanceId", humanInstanceId.Value);
        command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
        command.AddParameter($"@{paramPrefix}Parent", parent?.AsGuid ?? (object)DBNull.Value);
        command.AddParameter($"@{paramPrefix}Owner", owner?.AsGuid ?? (object)DBNull.Value);
        command.AddParameter($"@{paramPrefix}Effects", effects ?? (object)SqlBinary.Null);
        
        return command;
    }

    private string? _succeedFunctionSql;
    private string? _succeedFunctionWithEffectsSql;
    public StoreCommand SucceedFunction(StoredId storedId, byte[]? result, long timestamp, ReplicaId expectedReplica, string paramPrefix, byte[]? effects = null)
    {
        if (effects == null)
        {
            _succeedFunctionSql ??= @$"
                UPDATE {tablePrefix}
                SET Status = {(int)Status.Succeeded}, ResultJson = @ResultJson, Timestamp = @Timestamp, Owner = NULL
                WHERE Id = @Id AND Owner = @ExpectedReplica";

            var sql = paramPrefix == ""
                ? _succeedFunctionSql
                : _succeedFunctionSql.Replace("@", $"@{paramPrefix}");

            var command = StoreCommand.Create(sql);
            command.AddParameter($"@{paramPrefix}ResultJson", result ?? SqlBinary.Null);
            command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
            command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
            command.AddParameter($"@{paramPrefix}ExpectedReplica", expectedReplica.AsGuid);

            return command;
        }
        else
        {
            _succeedFunctionWithEffectsSql ??= @$"
                UPDATE {tablePrefix}
                SET Status = {(int)Status.Succeeded}, ResultJson = @ResultJson, Timestamp = @Timestamp, Owner = NULL, Effects = @Effects
                WHERE Id = @Id AND Owner = @ExpectedReplica";

            var sql = paramPrefix == ""
                ? _succeedFunctionWithEffectsSql
                : _succeedFunctionWithEffectsSql.Replace("@", $"@{paramPrefix}");

            var command = StoreCommand.Create(sql);
            command.AddParameter($"@{paramPrefix}ResultJson", result ?? SqlBinary.Null);
            command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
            command.AddParameter($"@{paramPrefix}Effects", effects);
            command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
            command.AddParameter($"@{paramPrefix}ExpectedReplica", expectedReplica.AsGuid);

            return command;
        }
    }
    
    private string? _postponedFunctionSql;
    private string? _postponedFunctionWithEffectsSql;
    public StoreCommand PostponeFunction(StoredId storedId, long postponeUntil, long timestamp, ReplicaId expectedReplica, string paramPrefix, byte[]? effects = null)
    {
        if (effects == null)
        {
            _postponedFunctionSql ??= @$"
                UPDATE {tablePrefix}
                SET Status = {(int)Status.Postponed},
                    Expires = CASE WHEN Interrupted = 1 THEN 0 ELSE @PostponedUntil END,
                    Timestamp = @Timestamp,
                    Owner = NULL,
                    Interrupted = 0
                WHERE Id = @Id AND Owner = @ExpectedReplica";

            var sql = paramPrefix == ""
                ? _postponedFunctionSql
                : _postponedFunctionSql.Replace("@", $"@{paramPrefix}");

            var command = StoreCommand.Create(sql);
            command.AddParameter($"@{paramPrefix}PostponedUntil", postponeUntil);
            command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
            command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
            command.AddParameter($"@{paramPrefix}ExpectedReplica", expectedReplica.AsGuid);

            return command;
        }
        else
        {
            _postponedFunctionWithEffectsSql ??= @$"
                UPDATE {tablePrefix}
                SET Status = {(int)Status.Postponed},
                    Expires = CASE WHEN Interrupted = 1 THEN 0 ELSE @PostponedUntil END,
                    Timestamp = @Timestamp,
                    Owner = NULL,
                    Interrupted = 0,
                    Effects = @Effects
                WHERE Id = @Id AND Owner = @ExpectedReplica";

            var sql = paramPrefix == ""
                ? _postponedFunctionWithEffectsSql
                : _postponedFunctionWithEffectsSql.Replace("@", $"@{paramPrefix}");

            var command = StoreCommand.Create(sql);
            command.AddParameter($"@{paramPrefix}PostponedUntil", postponeUntil);
            command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
            command.AddParameter($"@{paramPrefix}Effects", effects);
            command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
            command.AddParameter($"@{paramPrefix}ExpectedReplica", expectedReplica.AsGuid);

            return command;
        }
    }

    private string? _failFunctionSql;
    private string? _failFunctionWithEffectsSql;
    public StoreCommand FailFunction(StoredId storedId, StoredException storedException, long timestamp, ReplicaId expectedReplica, string paramPrefix, byte[]? effects = null)
    {
        if (effects == null)
        {
            _failFunctionSql ??= @$"
                UPDATE {tablePrefix}
                SET Status = {(int)Status.Failed}, ExceptionJson = @ExceptionJson, Timestamp = @timestamp, Owner = NULL
                WHERE Id = @Id AND Owner = @ExpectedReplica";

            var sql = paramPrefix == ""
                ? _failFunctionSql
                : _failFunctionSql.Replace("@", $"@{paramPrefix}");

            var command = StoreCommand.Create(sql);
            command.AddParameter($"@{paramPrefix}ExceptionJson", JsonSerializer.Serialize(storedException));
            command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
            command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
            command.AddParameter($"@{paramPrefix}ExpectedReplica", expectedReplica.AsGuid);

            return command;
        }
        else
        {
            _failFunctionWithEffectsSql ??= @$"
                UPDATE {tablePrefix}
                SET Status = {(int)Status.Failed}, ExceptionJson = @ExceptionJson, Timestamp = @timestamp, Owner = NULL, Effects = @Effects
                WHERE Id = @Id AND Owner = @ExpectedReplica";

            var sql = paramPrefix == ""
                ? _failFunctionWithEffectsSql
                : _failFunctionWithEffectsSql.Replace("@", $"@{paramPrefix}");

            var command = StoreCommand.Create(sql);
            command.AddParameter($"@{paramPrefix}ExceptionJson", JsonSerializer.Serialize(storedException));
            command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
            command.AddParameter($"@{paramPrefix}Effects", effects);
            command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
            command.AddParameter($"@{paramPrefix}ExpectedReplica", expectedReplica.AsGuid);

            return command;
        }
    }
    
    private string? _suspendFunctionSql;
    private string? _suspendFunctionWithEffectsSql;
    public StoreCommand SuspendFunction(
        StoredId storedId,
        long timestamp,
        ReplicaId expectedReplica,
        string paramPrefix,
        byte[]? effects = null
        )
    {
        if (effects == null)
        {
            _suspendFunctionSql ??= @$"
                    UPDATE {tablePrefix}
                    SET Status = CASE WHEN Interrupted = 1 THEN {(int)Status.Postponed} ELSE {(int)Status.Suspended} END,
                        Expires = 0,
                        Timestamp = @Timestamp,
                        Owner = NULL,
                        Interrupted = 0
                    WHERE Id = @Id AND Owner = @ExpectedReplica";

            var sql = paramPrefix == ""
                ? _suspendFunctionSql
                : _suspendFunctionSql.Replace("@", $"@{paramPrefix}");

            var command = StoreCommand.Create(sql);
            command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
            command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
            command.AddParameter($"@{paramPrefix}ExpectedReplica", expectedReplica.AsGuid);

            return command;
        }
        else
        {
            _suspendFunctionWithEffectsSql ??= @$"
                    UPDATE {tablePrefix}
                    SET Status = CASE WHEN Interrupted = 1 THEN {(int)Status.Postponed} ELSE {(int)Status.Suspended} END,
                        Expires = 0,
                        Timestamp = @Timestamp,
                        Owner = NULL,
                        Interrupted = 0,
                        Effects = @Effects
                    WHERE Id = @Id AND Owner = @ExpectedReplica";

            var sql = paramPrefix == ""
                ? _suspendFunctionWithEffectsSql
                : _suspendFunctionWithEffectsSql.Replace("@", $"@{paramPrefix}");

            var command = StoreCommand.Create(sql);
            command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
            command.AddParameter($"@{paramPrefix}Effects", effects);
            command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
            command.AddParameter($"@{paramPrefix}ExpectedReplica", expectedReplica.AsGuid);

            return command;
        }
    }

    private string? _restartExecutionSql;
    public StoreCommand RestartExecution(StoredId storedId, ReplicaId replicaId)
    {
        _restartExecutionSql ??= @$"
            UPDATE {tablePrefix}
            SET Status = {(int)Status.Executing},
                Expires = 0,
                Interrupted = 0,
                Owner = @Owner
            OUTPUT inserted.Id,
                   inserted.ParamJson,
                   inserted.Status,
                   inserted.ResultJson,
                   inserted.ExceptionJson,
                   inserted.Expires,
                   inserted.Interrupted,
                   inserted.Timestamp,
                   inserted.HumanInstanceId,
                   inserted.Parent,
                   inserted.Owner,
                   inserted.Effects
            WHERE Id = @Id AND Owner IS NULL;";

        var storeCommand = StoreCommand.Create(_restartExecutionSql);
        storeCommand.AddParameter("@Owner", replicaId.AsGuid);
        storeCommand.AddParameter("@Id", storedId.AsGuid);

        return storeCommand;
    }

    private string? _restartExecutionsSql;
    public StoreCommand RestartExecutions(IReadOnlyList<StoredId> storedIds, ReplicaId replicaId)
    {
        _restartExecutionsSql ??= @$"
            UPDATE {tablePrefix}
            SET Status = {(int)Status.Executing},
                Expires = 0,
                Interrupted = 0,
                Owner = @Owner
            OUTPUT inserted.Id,
                   inserted.ParamJson,
                   inserted.Status,
                   inserted.ResultJson,
                   inserted.ExceptionJson,
                   inserted.Expires,
                   inserted.Interrupted,
                   inserted.Timestamp,
                   inserted.HumanInstanceId,
                   inserted.Parent,
                   inserted.Owner,
                   inserted.Effects
            WHERE Id IN ({{0}}) AND Owner IS NULL;";

        var sql = string.Format(_restartExecutionsSql, storedIds.InClause());
        var storeCommand = StoreCommand.Create(sql);
        storeCommand.AddParameter("@Owner", replicaId.AsGuid);

        return storeCommand;
    }
    
    public StoredFlow? ReadToStoredFlow(StoredId storedId, SqlDataReader reader)
    {
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var id = reader.GetGuid(0).ToStoredId();
                var parameter = reader.IsDBNull(1) ? null : (byte[]) reader.GetValue(1);
                var status = (Status) reader.GetInt32(2);
                var result = reader.IsDBNull(3) ? null : (byte[]) reader.GetValue(3);
                var exceptionJson = reader.IsDBNull(4) ? null : reader.GetString(4);
                var storedException = exceptionJson == null
                    ? null
                    : JsonSerializer.Deserialize<StoredException>(exceptionJson);
                var expires = reader.GetInt64(5);
                var interrupted = reader.GetBoolean(6);
                var timestamp = reader.GetInt64(7);
                var humanInstanceId = reader.GetString(8);
                var parentId = reader.IsDBNull(9) ? null : reader.GetGuid(9).ToStoredId();
                var ownerId = reader.IsDBNull(10) ? null : reader.GetGuid(10).ToReplicaId();

                return new StoredFlow(
                    id,
                    humanInstanceId,
                    parameter,
                    status,
                    storedException,
                    expires,
                    timestamp,
                    interrupted,
                    parentId,
                    ownerId,
                    storedId.Type
                );
            }
        }

        return null;
    }

    public (StoredFlow?, byte[]?) ReadToStoredFlowWithEffects(StoredId storedId, SqlDataReader reader)
    {
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var id = reader.GetGuid(0).ToStoredId();
                var parameter = reader.IsDBNull(1) ? null : (byte[])reader.GetValue(1);
                var status = (Status)reader.GetInt32(2);
                var result = reader.IsDBNull(3) ? null : (byte[])reader.GetValue(3);
                var exceptionJson = reader.IsDBNull(4) ? null : reader.GetString(4);
                var storedException = exceptionJson == null
                    ? null
                    : JsonSerializer.Deserialize<StoredException>(exceptionJson);
                var expires = reader.GetInt64(5);
                var interrupted = reader.GetBoolean(6);
                var timestamp = reader.GetInt64(7);
                var humanInstanceId = reader.GetString(8);
                var parentId = reader.IsDBNull(9) ? null : reader.GetGuid(9).ToStoredId();
                var ownerId = reader.IsDBNull(10) ? null : reader.GetGuid(10).ToReplicaId();
                var hasEffects = !reader.IsDBNull(11);
                var effectsBytes = hasEffects ? (byte[])reader.GetValue(11) : null;

                var storedFlow = new StoredFlow(
                    id,
                    humanInstanceId,
                    parameter,
                    status,
                    storedException,
                    expires,
                    timestamp,
                    interrupted,
                    parentId,
                    ownerId,
                    storedId.Type
                );

                return (storedFlow, effectsBytes);
            }
        }

        return (null, null);
    }
    
    public StoreCommand? AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages, string prefix = "")
    {
        if (messages.Count == 0)
            return null;

        var interruptCommand = Interrupt(messages.Select(m => m.StoredId).Distinct().ToList());

        var sql = @$"
            INSERT INTO {tablePrefix}_Messages
                (Id, Position, Content)
            VALUES
                 {messages.Select((_, i) => $"(@{prefix}Id{i}, @{prefix}Position{i}, @{prefix}Content{i})").StringJoin($",{Environment.NewLine}")};";

        var appendCommand = StoreCommand.Create(sql);
        for (var i = 0; i < messages.Count; i++)
        {
            var (storedId, (messageContent, messageType, _, idempotencyKey), position) = messages[i];
            var content = BinaryPacker.Pack(
                messageContent,
                messageType,
                idempotencyKey?.ToUtf8Bytes()
            );
            appendCommand.AddParameter($"@{prefix}Id{i}", storedId.AsGuid);
            appendCommand.AddParameter($"@{prefix}Position{i}", position);
            appendCommand.AddParameter($"@{prefix}Content{i}", content);
        }

        return StoreCommand.Merge(appendCommand, interruptCommand);
    }
    
    private string? _getMessagesSql;
    public StoreCommand GetMessages(StoredId storedId, long skip, string paramPrefix = "")
    {
        _getMessagesSql ??= @$"
            SELECT Content, Position
            FROM {tablePrefix}_Messages
            WHERE Id = @Id AND Position >= @Position
            ORDER BY Position ASC;";

        var sql = _getMessagesSql;
        if (paramPrefix != "")
            sql = sql.Replace("@", $"@{paramPrefix}");
        var command = StoreCommand.Create(sql);
        command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
        command.AddParameter($"@{paramPrefix}Position", skip);

        return command;
    }

    public StoreCommand GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions)
    {
        var positionsClause = skipPositions.Select(p => p.ToString()).StringJoin(", ");
        var sql = @$"
            SELECT Content, Position
            FROM {tablePrefix}_Messages
            WHERE Id = @Id AND Position NOT IN ({positionsClause});";

        var command = StoreCommand.Create(sql);
        command.AddParameter("@Id", storedId.AsGuid);

        return command;
    }

    public async Task<IReadOnlyList<(byte[] content, long position)>> ReadMessages(SqlDataReader reader)
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
            SELECT Id, Position, Content
            FROM {tablePrefix}_Messages
            WHERE Id IN ({storedIds.InClause()});";

        var command = StoreCommand.Create(sql);
        return command;
    }
    
    public async Task<Dictionary<StoredId, List<(byte[] content, long position)>>> ReadStoredIdsMessages(SqlDataReader reader)
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
        var positionsList = positions.ToList();
        var sql = @$"
            DELETE FROM {tablePrefix}_Messages
            WHERE Id = @Id AND Position IN ({positionsList.Select((_, i) => $"@Position{i}").StringJoin(", ")})";

        var command = StoreCommand.Create(sql);
        command.AddParameter("@Id", storedId.AsGuid);
        for (var i = 0; i < positionsList.Count; i++)
            command.AddParameter($"@Position{i}", positionsList[i]);

        return command;
    }
}