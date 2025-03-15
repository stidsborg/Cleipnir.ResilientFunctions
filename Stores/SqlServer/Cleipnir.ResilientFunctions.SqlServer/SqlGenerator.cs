using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlGenerator(string tablePrefix)
{
    public StoreCommand Interrupt(IEnumerable<StoredId> storedIds)
    {
        var conditionals = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(group => $"(FlowType = {group.Key} AND FlowInstance IN ({group.Select(i => $"'{i}'").StringJoin(", ")}))")
            .StringJoin(" OR ");
        
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
                WHERE {conditionals};";

        return new StoreCommand(sql);
    }
    
    public StoreCommand UpdateEffects(IReadOnlyList<StoredEffectChange> changes, string paramPrefix)
    {
        var stringBuilder = new StringBuilder(capacity: 2);
        var upserts = changes
            .Where(c => c.Operation == CrudOperation.Upsert)
            .Select(c => new
            {
                Type = c.StoredId.Type.Value, 
                Instance = c.StoredId.Instance.Value, 
                StoredEffectId = c.EffectId.Value,
                WorkStatus = (int)c.StoredEffect!.WorkStatus, 
                Result = c.StoredEffect!.Result,
                Exception = c.StoredEffect!.StoredException, 
                EffectId = c.StoredEffect!.EffectId
            })
            .ToList();

        var parameterValues = upserts
            .Select((_, i) => $"(@{paramPrefix}FlowType{i}, @{paramPrefix}FlowInstance{i}, @{paramPrefix}StoredId{i}, @{paramPrefix}EffectId{i}, @{paramPrefix}Status{i}, @{paramPrefix}Result{i}, @{paramPrefix}Exception{i})")
            .StringJoin(", ");
        
        var setSql = $@"
            MERGE INTO {tablePrefix}_Effects
                USING (VALUES {parameterValues}) 
                AS source (FlowType, FlowInstance, StoredId, EffectId, Status, Result, Exception)
                ON {tablePrefix}_Effects.FlowType = source.FlowType AND {tablePrefix}_Effects.FlowInstance = source.FlowInstance AND {tablePrefix}_Effects.StoredId = source.StoredId
                WHEN MATCHED THEN
                    UPDATE SET Status = source.Status, Result = source.Result, Exception = source.Exception 
                WHEN NOT MATCHED THEN
                    INSERT (FlowType, FlowInstance, StoredId, EffectId, Status, Result, Exception)
                    VALUES (source.FlowType, source.FlowInstance, source.StoredId, source.EffectId, source.Status, source.Result, source.Exception);";
        
        if (upserts.Any())
            stringBuilder.AppendLine(setSql);

        var parameters = new List<ParameterValueAndName>();
        
        for (var i = 0; i < upserts.Count; i++)
        {
            var upsert = upserts[i];
            parameters.Add(new ParameterValueAndName($"@{paramPrefix}FlowType{i}", upsert.Type));
            parameters.Add(new ParameterValueAndName($"@{paramPrefix}FlowInstance{i}", upsert.Instance));
            parameters.Add(new ParameterValueAndName($"@{paramPrefix}StoredId{i}", upsert.StoredEffectId));
            parameters.Add(new ParameterValueAndName($"@{paramPrefix}EffectId{i}", upsert.EffectId.Serialize()));
            parameters.Add(new ParameterValueAndName($"@{paramPrefix}Status{i}", upsert.WorkStatus));
            parameters.Add(new ParameterValueAndName($"@{paramPrefix}Result{i}", upsert.Result ?? (object) SqlBinary.Null));
            parameters.Add(new ParameterValueAndName($"@{paramPrefix}Exception{i}", JsonHelper.ToJson(upsert.Exception) ?? (object) DBNull.Value));
        }

        var removes = changes
            .Where(c => c.Operation == CrudOperation.Delete)
            .Select(c => new { Type = c.StoredId.Type.Value, Instance = c.StoredId.Instance.Value, IdHash = c.EffectId.Value })
            .GroupBy(a => new {a.Type, a.Instance }, a => a.IdHash)
            .ToList();
        var predicates = removes
            .Select(r =>
                $"(FlowType = {r.Key.Type} AND FlowInstance = '{r.Key.Instance}' AND StoredId IN ({r.Select(id => $"'{id}'").StringJoin(", ")}))")
            .StringJoin($" OR {Environment.NewLine}");
        var removeSql = @$"
            DELETE FROM {tablePrefix}_effects 
            WHERE {predicates}";
        if (removes.Any())
            stringBuilder.AppendLine(removeSql);

        return new StoreCommand(
            sql: stringBuilder.ToString(),
            parameters
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
        string? paramPrefix)
    {
        _createFunctionSql ??= @$"
                INSERT INTO {tablePrefix}(
                    FlowType, FlowInstance, 
                    ParamJson, 
                    Status,
                    Epoch, 
                    Expires,
                    Timestamp,
                    HumanInstanceId,
                    Parent
                )
                VALUES
                (
                    @FlowType, @flowInstance, 
                    @ParamJson,   
                    @Status,
                    0,
                    @Expires,
                    @Timestamp,
                    @HumanInstanceId,
                    @Parent
                )";

        var sql = _createFunctionSql;
        if (paramPrefix != null)
            sql = sql.Replace("@", $"@{paramPrefix}");

        var command = new StoreCommand(sql);
        command.AddParameter($"@{paramPrefix}FlowType", storedId.Type.Value);
        command.AddParameter($"@{paramPrefix}FlowInstance", storedId.Instance.Value);
        command.AddParameter($"@{paramPrefix}Status", (int)(postponeUntil == null ? Status.Executing : Status.Postponed));
        command.AddParameter($"@{paramPrefix}ParamJson", param == null ? SqlBinary.Null : param);
        command.AddParameter($"@{paramPrefix}Expires", postponeUntil ?? leaseExpiration);
        command.AddParameter($"@{paramPrefix}HumanInstanceId", humanInstanceId.Value);
        command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
        command.AddParameter($"@{paramPrefix}Parent", parent?.Serialize() ?? (object)DBNull.Value);

        return command;
    }

    private string? _succeedFunctionSql;
    public StoreCommand SucceedFunction(StoredId storedId, byte[]? result, long timestamp, int expectedEpoch, string paramPrefix)
    {
        _succeedFunctionSql ??= @$"
            UPDATE {tablePrefix}
            SET Status = {(int) Status.Succeeded}, ResultJson = @ResultJson, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Epoch = @ExpectedEpoch";
        
        var sql = paramPrefix == "" 
            ? _succeedFunctionSql
            : _succeedFunctionSql.Replace("@", $"@{paramPrefix}");

        var command = new StoreCommand(sql);
        command.AddParameter($"@{paramPrefix}ResultJson", result ?? SqlBinary.Null);
        command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
        command.AddParameter($"@{paramPrefix}FlowInstance", storedId.Instance.Value);
        command.AddParameter($"@{paramPrefix}FlowType", storedId.Type.Value);
        command.AddParameter($"@{paramPrefix}ExpectedEpoch", expectedEpoch);

        return command;
    }
    
    private string? _postponedFunctionSql;
    public StoreCommand PostponeFunction(StoredId storedId, long postponeUntil, long timestamp, bool ignoreInterrupted, int expectedEpoch, string paramPrefix)
    {
        _postponedFunctionSql ??= @$"
            UPDATE {tablePrefix}
            SET Status = {(int) Status.Postponed}, Expires = @PostponedUntil, Timestamp = @Timestamp, Epoch = @ExpectedEpoch
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Epoch = @ExpectedEpoch AND Interrupted = 0";
        
        var sql = paramPrefix == "" 
            ? _postponedFunctionSql
            : _postponedFunctionSql.Replace("@", $"@{paramPrefix}");
        
        if (ignoreInterrupted)
            sql = sql.Replace("Interrupted = 0", "1 = 1");

        var command = new StoreCommand(sql);
        
        command.AddParameter($"@{paramPrefix}PostponedUntil", postponeUntil);
        command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
        command.AddParameter($"@{paramPrefix}FlowInstance", storedId.Instance.Value);
        command.AddParameter($"@{paramPrefix}FlowType", storedId.Type.Value);
        command.AddParameter($"@{paramPrefix}ExpectedEpoch", expectedEpoch);

        return command;
    }

    private string? _failFunctionSql;
    public StoreCommand FailFunction(StoredId storedId, StoredException storedException, long timestamp, int expectedEpoch, string paramPrefix)
    {
        _failFunctionSql ??= @$"
            UPDATE {tablePrefix}
            SET Status = {(int) Status.Failed}, ExceptionJson = @ExceptionJson, Timestamp = @timestamp, Epoch = @ExpectedEpoch
            WHERE FlowType = @FlowType
            AND FlowInstance = @FlowInstance
            AND Epoch = @ExpectedEpoch";

        var sql = paramPrefix == "" 
            ? _failFunctionSql
            : _failFunctionSql.Replace("@", $"@{paramPrefix}");

        var command = new StoreCommand(sql);
        command.AddParameter($"@{paramPrefix}ExceptionJson", JsonSerializer.Serialize(storedException));
        command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
        command.AddParameter($"@{paramPrefix}FlowInstance", storedId.Instance.Value);
        command.AddParameter($"@{paramPrefix}FlowType", storedId.Type.Value);
        command.AddParameter($"@{paramPrefix}ExpectedEpoch", expectedEpoch);

        return command;
    }
    
    private string? _suspendFunctionSql;
    public StoreCommand SuspendFunction(
        StoredId storedId, 
        long timestamp,
        int expectedEpoch, 
        string paramPrefix
        )
    {
        _suspendFunctionSql ??= @$"
                UPDATE {tablePrefix}
                SET Status = {(int)Status.Suspended}, Timestamp = @Timestamp
                WHERE FlowType = @FlowType AND 
                      FlowInstance = @FlowInstance AND                       
                      Epoch = @ExpectedEpoch AND
                      Interrupted = 0;";
        
        var sql = paramPrefix == "" 
            ? _suspendFunctionSql
            : _suspendFunctionSql.Replace("@", $"@{paramPrefix}");
        
        var command = new StoreCommand(sql);
        command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
        command.AddParameter($"@{paramPrefix}FlowType", storedId.Type.Value);
        command.AddParameter($"@{paramPrefix}FlowInstance", storedId.Instance.Value);
        command.AddParameter($"@{paramPrefix}ExpectedEpoch", expectedEpoch);

        return command;
    }
    
    public StoreCommand? AppendMessages(IReadOnlyList<StoredIdAndMessageWithPosition> messages, bool interrupt)
    {
        if (messages.Count == 0)
            return null;
        
        var interruptCommand = interrupt 
            ? Interrupt(messages.Select(m => m.StoredId).Distinct().ToList()) 
            : null;
        
        var sql = @$"    
            INSERT INTO {tablePrefix}_Messages
                (FlowType, FlowInstance, Position, MessageJson, MessageType, IdempotencyKey)
            VALUES 
                 {messages.Select((_, i) => $"(@FlowType{i}, @FlowInstance{i}, @Position{i}, @MessageJson{i}, @MessageType{i}, @IdempotencyKey{i})").StringJoin($",{Environment.NewLine}")};";

        var appendCommand = new StoreCommand(sql);
        for (var i = 0; i < messages.Count; i++)
        {
            var (storedId, (messageContent, messageType, idempotencyKey), position) = messages[i];
            var (storedType, storedInstance) = storedId;
            appendCommand.AddParameter($"@FlowType{i}", storedType.Value);
            appendCommand.AddParameter($"@FlowInstance{i}", storedInstance.Value);
            appendCommand.AddParameter($"@Position{i}", position);
            appendCommand.AddParameter($"@MessageJson{i}", messageContent);
            appendCommand.AddParameter($"@MessageType{i}", messageType);
            appendCommand.AddParameter($"@IdempotencyKey{i}", idempotencyKey ?? (object)DBNull.Value);
        }

        return StoreCommand.Merge(appendCommand, interruptCommand);
    }
}