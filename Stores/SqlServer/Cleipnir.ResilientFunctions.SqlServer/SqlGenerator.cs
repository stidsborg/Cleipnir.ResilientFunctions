using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
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
    
    public StoreCommand? UpdateEffects(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, PositionsStorageSession session, string paramPrefix)
    {
        var storeCommands = new List<StoreCommand>(2);

        // DELETES
        {
            var positionsToDelete = changes
                .Select(c =>
                    session.Positions.ContainsKey(c.EffectId.Serialize())
                        ? session.Positions[c.EffectId.Serialize()]
                        : -1)
                .Where(p => p != -1)
                .ToList();

            if (positionsToDelete.Any())
            {
                var removeSql = @$"
                DELETE FROM {tablePrefix}_Effects
                WHERE Id = '{storedId.AsGuid}' AND Position IN ({positionsToDelete.Select(p => p.ToString()).StringJoin(separator: ", ")});";
                storeCommands.Add(StoreCommand.Create(removeSql));
            }
        }

        // INSERT and UPDATES
        {
            var insertsAndUpdates = changes
                .Where(c => c.Operation == CrudOperation.Insert || c.Operation == CrudOperation.Update)
                .ToList();

            for (var i = 0; i < insertsAndUpdates.Count; i++)
            {
                var (_, effectId, _, storedEffect) = insertsAndUpdates[i];
                var position = session.Add(effectId.Serialize());

                var sql = $@"
                INSERT INTO {tablePrefix}_Effects
                    (Id, Position, EffectId, Status, Result, Exception)
                VALUES
                    (@{paramPrefix}Id{i}, @{paramPrefix}Position{i}, @{paramPrefix}EffectId{i}, @{paramPrefix}Status{i}, @{paramPrefix}Result{i}, @{paramPrefix}Exception{i});";

                var command = StoreCommand.Create(sql);
                command.AddParameter($"@{paramPrefix}Id{i}", storedId.AsGuid);
                command.AddParameter($"@{paramPrefix}Position{i}", position);
                command.AddParameter($"@{paramPrefix}EffectId{i}", storedEffect!.EffectId.Serialize().Value);
                command.AddParameter($"@{paramPrefix}Status{i}", (int)storedEffect.WorkStatus);
                command.AddParameter($"@{paramPrefix}Result{i}", storedEffect.Result ?? (object)SqlBinary.Null);
                command.AddParameter($"@{paramPrefix}Exception{i}", JsonHelper.ToJson(storedEffect.StoredException) ?? (object)DBNull.Value);

                storeCommands.Add(command);
            }
        }

        return storeCommands.Count == 0
            ? null
            : StoreCommand.Merge(storeCommands)!;
    }
    
    private string? _getEffectsSql;
    public StoreCommand GetEffects(StoredId storedId, string paramPrefix = "")
    {
        _getEffectsSql ??= @$"
            SELECT Position, EffectId, Status, Result, Exception           
            FROM {tablePrefix}_Effects
            WHERE Id = @Id";

        var sql = _getEffectsSql;
        if (paramPrefix != "")
            sql = sql.Replace("@", $"@{paramPrefix}");
        
        var command = StoreCommand.Create(sql);
        command.AddParameter($"@{paramPrefix}Id", storedId.AsGuid);
        return command;
    }
    
    public async Task<IReadOnlyList<StoredEffect>> ReadEffects(SqlDataReader reader)
        => (await ReadEffectsWithPositions(reader)).Select(e => e.Effect).ToList();

    public async Task<IReadOnlyList<StoredEffectWithPosition>> ReadEffectsWithPositions(SqlDataReader reader)
    {
        var storedEffects = new List<StoredEffectWithPosition>();
        while (reader.HasRows && await reader.ReadAsync())
        {
            var position = reader.GetInt64(0);
            var effectId = reader.GetString(1);

            var status = (WorkStatus)reader.GetInt32(2);
            var result = reader.IsDBNull(3) ? null : (byte[])reader.GetValue(3);
            var exception = reader.IsDBNull(4) ? null : reader.GetString(4);

            var storedException = exception == null ? null : JsonSerializer.Deserialize<StoredException>(exception);
            var storedEffect = new StoredEffect(EffectId.Deserialize(effectId), status, result, storedException);
            storedEffects.Add(new StoredEffectWithPosition(storedEffect, position));
        }

        return storedEffects;
    }
    
    public StoreCommand GetEffects(IEnumerable<StoredId> storedIds)
    {
        var sql = @$"
            SELECT Id, Position, EffectId, Status, Result, Exception           
            FROM {tablePrefix}_Effects
            WHERE Id IN ({storedIds.InClause()})";
        
        var command = StoreCommand.Create(sql);
        return command;
    }
    
    public async Task<Dictionary<StoredId, List<StoredEffectWithPosition>>> ReadEffectsForMultipleStoredIds(SqlDataReader reader, IEnumerable<StoredId> storedIds)
    {
        var storedEffects = new Dictionary<StoredId, List<StoredEffectWithPosition>>();
        foreach (var storedId in storedIds)
            storedEffects[storedId] = new List<StoredEffectWithPosition>();

        while (reader.HasRows && await reader.ReadAsync())
        {
            var storedId = reader.GetGuid(0).ToStoredId();
            var position = reader.GetInt64(1);
            var effectId = reader.GetString(2);

            var status = (WorkStatus)reader.GetInt32(3);
            var result = reader.IsDBNull(4) ? null : (byte[])reader.GetValue(4);
            var exception = reader.IsDBNull(5) ? null : reader.GetString(5);

            var storedException = exception == null ? null : JsonSerializer.Deserialize<StoredException>(exception);
            var storedEffect = new StoredEffect(EffectId.Deserialize(effectId), status, result, storedException);

            storedEffects[storedId].Add(new StoredEffectWithPosition(storedEffect, position));
        }

        return storedEffects;
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
                    result,
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