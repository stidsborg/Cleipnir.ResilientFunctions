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
    
    public StoreCommand UpdateEffects(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, SnapshotStorageSession session, string paramPrefix)
    {
        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                session.Effects.Remove(change.EffectId);
            else
                session.Effects[change.EffectId] = change.StoredEffect!;

        var content = session.Serialize();
        if (!session.RowExists)
        {
            session.RowExists = true;
            var insertSql = $@"INSERT INTO {tablePrefix}_Effects
                            (Id, Position, Content, Version)
                       VALUES
                            (@{paramPrefix}Id, 0, @{paramPrefix}Content, 0);";
            var insertCommand = StoreCommand.Create(insertSql);
            insertCommand.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
            insertCommand.AddParameter($"@{paramPrefix}Content", content);
            return insertCommand;
        }

        var sql = $@"
            UPDATE {tablePrefix}_Effects
            SET Content = @{paramPrefix}Content, Version = Version + 1
            WHERE Id = @{paramPrefix}Id AND Position = 0 AND Version = @{paramPrefix}Version;";

        var command = StoreCommand.Create(sql);
        command.AddParameter($"@{paramPrefix}Content", content);
        command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
        command.AddParameter($"@{paramPrefix}Version", session.Version++);

        return command;
    }
    
    private string? _getEffectsSql;
    public StoreCommand GetEffects(StoredId storedId, string paramPrefix = "")
    {
        _getEffectsSql ??= @$"
            SELECT Id, Position, Content, Version
            FROM {tablePrefix}_Effects
            WHERE Id = @Id";

        var sql = _getEffectsSql;
        if (paramPrefix != "")
            sql = sql.Replace("@", $"@{paramPrefix}");

        var command = StoreCommand.Create(sql);
        command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
        return command;
    }

    public record StoredEffectsWithSession(IReadOnlyList<StoredEffect> Effects, SnapshotStorageSession Session);
    public async Task<StoredEffectsWithSession> ReadEffects(SqlDataReader reader)
    {
        var effects = new List<StoredEffect>();
        var session = new SnapshotStorageSession();

        while (reader.HasRows && await reader.ReadAsync())
        {
            var id = reader.GetGuid(0);
            var position = reader.GetInt32(1);
            var content = (byte[])reader.GetValue(2);
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
    
    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT Id, Position, Content, Version
            FROM {tablePrefix}_Effects
            WHERE Id IN ({storedIds.InClause()})";

        var command = StoreCommand.Create(sql);
        return command;
    }

    public async Task<Dictionary<StoredId, SnapshotStorageSession>> ReadEffectsForMultipleStoredIds(SqlDataReader reader, IEnumerable<StoredId> storedIds)
    {
        var effects = new Dictionary<StoredId, SnapshotStorageSession>();
        foreach (var storedId in storedIds)
            effects[storedId] = new SnapshotStorageSession();

        while (reader.HasRows && await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
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
        string? paramPrefix)
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
                    Owner                                                          
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
                    @Owner
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
        command.AddParameter($"@{paramPrefix}Parent", parent?.Serialize() ?? (object)DBNull.Value);
        command.AddParameter($"@{paramPrefix}Owner", owner?.AsGuid ?? (object)DBNull.Value);

        return command;
    }

    private string? _succeedFunctionSql;
    public StoreCommand SucceedFunction(StoredId storedId, byte[]? result, long timestamp, ReplicaId expectedReplica, string paramPrefix)
    {
        _succeedFunctionSql ??= @$"
            UPDATE {tablePrefix}
            SET Status = {(int) Status.Succeeded}, ResultJson = @ResultJson, Timestamp = @Timestamp, Owner = NULL
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
    
    private string? _postponedFunctionSql;
    public StoreCommand PostponeFunction(StoredId storedId, long postponeUntil, long timestamp, ReplicaId expectedReplica, string paramPrefix)
    {
        _postponedFunctionSql ??= @$"
            UPDATE {tablePrefix}
            SET Status = {(int) Status.Postponed},
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

    private string? _failFunctionSql;
    public StoreCommand FailFunction(StoredId storedId, StoredException storedException, long timestamp, ReplicaId expectedReplica, string paramPrefix)
    {
        _failFunctionSql ??= @$"
            UPDATE {tablePrefix}
            SET Status = {(int) Status.Failed}, ExceptionJson = @ExceptionJson, Timestamp = @timestamp, Owner = NULL
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
    
    private string? _suspendFunctionSql;
    public StoreCommand SuspendFunction(
        StoredId storedId,
        long timestamp,
        ReplicaId expectedReplica,
        string paramPrefix
        )
    {
        _suspendFunctionSql ??= @$"
                UPDATE {tablePrefix}
                SET Status = CASE WHEN Interrupted = 1 THEN {(int) Status.Postponed} ELSE {(int) Status.Suspended} END,
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
                   inserted.Owner
            WHERE Id = @Id AND Owner IS NULL;";

        var storeCommand = StoreCommand.Create(_restartExecutionSql);
        storeCommand.AddParameter("@Owner", replicaId.AsGuid);
        storeCommand.AddParameter("@Id", storedId.AsGuid);

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
                var parentId = reader.IsDBNull(9) ? null : StoredId.Deserialize(reader.GetString(9));
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
    
    public StoreCommand? AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages, bool interrupt, string prefix = "")
    {
        if (messages.Count == 0)
            return null;
        
        var interruptCommand = interrupt 
            ? Interrupt(messages.Select(m => m.StoredId).Distinct().ToList()) 
            : null;
        
        var sql = @$"    
            INSERT INTO {tablePrefix}_Messages
                (Id, Position, MessageJson, MessageType, IdempotencyKey)
            VALUES 
                 {messages.Select((_, i) => $"(@{prefix}Id{i}, @{prefix}Position{i}, @{prefix}MessageJson{i}, @{prefix}MessageType{i}, @{prefix}IdempotencyKey{i})").StringJoin($",{Environment.NewLine}")};";

        var appendCommand = StoreCommand.Create(sql);
        for (var i = 0; i < messages.Count; i++)
        {
            var (storedId, (messageContent, messageType, idempotencyKey), position) = messages[i];
            appendCommand.AddParameter($"@{prefix}Id{i}", storedId.AsGuid);
            appendCommand.AddParameter($"@{prefix}Position{i}", position);
            appendCommand.AddParameter($"@{prefix}MessageJson{i}", messageContent);
            appendCommand.AddParameter($"@{prefix}MessageType{i}", messageType);
            appendCommand.AddParameter($"@{prefix}IdempotencyKey{i}", idempotencyKey ?? (object)DBNull.Value);
        }

        return StoreCommand.Merge(appendCommand, interruptCommand);
    }
    
    private string? _getMessagesSql;
    public StoreCommand GetMessages(StoredId storedId, int skip, string paramPrefix = "")
    {
        _getMessagesSql ??= @$"    
            SELECT MessageJson, MessageType, IdempotencyKey
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
    
    public async Task<IReadOnlyList<StoredMessage>> ReadMessages(SqlDataReader reader)
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
            SELECT Id, Position, MessageJson, MessageType, IdempotencyKey
            FROM {tablePrefix}_Messages
            WHERE Id IN ({storedIds.InClause()});";
        
        var command = StoreCommand.Create(sql);
        return command;
    }
    
    public async Task<Dictionary<StoredId, List<StoredMessage>>> ReadStoredIdsMessages(SqlDataReader reader)
    {
        var storedMessages = new Dictionary<StoredId, List<StoredMessageWithPosition>>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
            var position = reader.GetInt32(1);
            var messageJson = (byte[]) reader.GetValue(2);
            var messageType = (byte[]) reader.GetValue(3);
            var idempotencyKey = reader.IsDBNull(4) ? null : reader.GetString(4);

            if (!storedMessages.ContainsKey(id))
                storedMessages[id] = new List<StoredMessageWithPosition>();

            var storedMessage = new StoredMessage(messageJson, messageType, idempotencyKey);
            storedMessages[id].Add(new StoredMessageWithPosition(storedMessage, position));
        }
        
        return storedMessages.ToDictionary(kv => kv.Key, kv => kv.Value.OrderBy(m => m.Position).Select(m => m.StoredMessage).ToList());
    }
}