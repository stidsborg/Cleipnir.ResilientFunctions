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

        return StoreCommand.Create(sql);
    }
    
    public StoreCommand UpdateEffects(IReadOnlyList<StoredEffectChange> changes, string paramPrefix)
    {
        var storeCommands = new List<StoreCommand>(3);
        
        //INSERTION
        {
            var inserts = changes
                .Where(c => c.Operation == CrudOperation.Insert)
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
        
            var parameterValues = inserts
                .Select((_, i) => $"(@{paramPrefix}InsertionFlowType{i}, @{paramPrefix}InsertionFlowInstance{i}, @{paramPrefix}InsertionStoredId{i}, @{paramPrefix}InsertionEffectId{i}, @{paramPrefix}InsertionStatus{i}, @{paramPrefix}InsertionResult{i}, @{paramPrefix}InsertionException{i})")
                .StringJoin(", ");
        
            var parameters = new List<ParameterValueAndName>();
            for (var i = 0; i < inserts.Count; i++)
            {
                var upsert = inserts[i];
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}InsertionFlowType{i}", upsert.Type));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}InsertionFlowInstance{i}", upsert.Instance));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}InsertionStoredId{i}", upsert.StoredEffectId));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}InsertionEffectId{i}", upsert.EffectId.Serialize()));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}InsertionStatus{i}", upsert.WorkStatus));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}InsertionResult{i}",
                    upsert.Result ?? (object)SqlBinary.Null));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}InsertionException{i}",
                    JsonHelper.ToJson(upsert.Exception) ?? (object)DBNull.Value));
            }
            
            var insertSql = $@"
            INSERT INTO {tablePrefix}_Effects
            VALUES {parameterValues} ";
            
            if (inserts.Any())
                storeCommands.Add(StoreCommand.Create(insertSql, parameters));
        }

        //UPDATE
        {
            var upserts = changes
                .Where(c => c.Operation == CrudOperation.Update)
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
                .Select((_, i) =>
                    $"(@{paramPrefix}FlowType{i}, @{paramPrefix}FlowInstance{i}, @{paramPrefix}StoredId{i}, @{paramPrefix}EffectId{i}, @{paramPrefix}Status{i}, @{paramPrefix}Result{i}, @{paramPrefix}Exception{i})")
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

            var parameters = new List<ParameterValueAndName>();

            for (var i = 0; i < upserts.Count; i++)
            {
                var upsert = upserts[i];
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}FlowType{i}", upsert.Type));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}FlowInstance{i}", upsert.Instance));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}StoredId{i}", upsert.StoredEffectId));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}EffectId{i}", upsert.EffectId.Serialize()));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}Status{i}", upsert.WorkStatus));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}Result{i}",
                    upsert.Result ?? (object)SqlBinary.Null));
                parameters.Add(new ParameterValueAndName($"@{paramPrefix}Exception{i}",
                    JsonHelper.ToJson(upsert.Exception) ?? (object)DBNull.Value));
            }
            
            if (upserts.Any())
                storeCommands.Add(StoreCommand.Create(setSql, parameters));
        }
        
        //DELETE
        {
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
                storeCommands.Add(StoreCommand.Create(removeSql));
            
        }

        return StoreCommand.Merge(storeCommands)!;
    }
    
    private string? _getEffectsSql;
    public StoreCommand GetEffects(StoredId storedId, string paramPrefix = "")
    {
        _getEffectsSql ??= @$"
            SELECT StoredId, EffectId, Status, Result, Exception           
            FROM {tablePrefix}_Effects
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";

        var sql = _getEffectsSql;
        if (paramPrefix != "")
            sql = sql.Replace("@", $"@{paramPrefix}");
        
        var command = StoreCommand.Create(sql);
        command.AddParameter($"@{paramPrefix}FlowType", storedId.Type.Value);
        command.AddParameter($"@{paramPrefix}FlowInstance", storedId.Instance.Value);
        return command;
    }
    
    public async Task<IReadOnlyList<StoredEffect>> ReadEffects(SqlDataReader reader)
    {
        var storedEffects = new List<StoredEffect>();
        while (reader.HasRows && await reader.ReadAsync())
        {
            var storedEffectId = reader.GetGuid(0);
            var effectId = reader.GetString(1);
            
            var status = (WorkStatus) reader.GetInt32(2);
            var result = reader.IsDBNull(3) ? null : (byte[]) reader.GetValue(3);
            var exception = reader.IsDBNull(4) ? null : reader.GetString(4);

            var storedException = exception == null ? null : JsonSerializer.Deserialize<StoredException>(exception);
            var storedEffect = new StoredEffect(EffectId.Deserialize(effectId), new StoredEffectId(storedEffectId), status, result, storedException);
            storedEffects.Add(storedEffect);
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

        var command = StoreCommand.Create(sql);
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

        var command = StoreCommand.Create(sql);
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

        var command = StoreCommand.Create(sql);
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

        var command = StoreCommand.Create(sql);
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
        
        var command = StoreCommand.Create(sql);
        command.AddParameter($"@{paramPrefix}Timestamp", timestamp);
        command.AddParameter($"@{paramPrefix}FlowType", storedId.Type.Value);
        command.AddParameter($"@{paramPrefix}FlowInstance", storedId.Instance.Value);
        command.AddParameter($"@{paramPrefix}ExpectedEpoch", expectedEpoch);

        return command;
    }
    
    private string? _restartExecutionSql;
    public StoreCommand RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration)
    {
        _restartExecutionSql ??= @$"
            UPDATE {tablePrefix}
            SET Epoch = Epoch + 1, 
                Status = {(int)Status.Executing}, 
                Expires = @LeaseExpiration,
                Interrupted = 0
            OUTPUT inserted.ParamJson,                
                   inserted.Status,
                   inserted.ResultJson, 
                   inserted.ExceptionJson,                   
                   inserted.Expires,
                   inserted.Epoch,
                   inserted.Interrupted,
                   inserted.Timestamp,
                   inserted.HumanInstanceId,
                   inserted.Parent
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Epoch = @ExpectedEpoch;";

        var storeCommand = StoreCommand.Create(_restartExecutionSql);
        storeCommand.AddParameter("@LeaseExpiration", leaseExpiration);
        storeCommand.AddParameter("@FlowType", storedId.Type.Value);
        storeCommand.AddParameter("@FlowInstance", storedId.Instance.Value);
        storeCommand.AddParameter("@ExpectedEpoch", expectedEpoch);

        return storeCommand;
    }
    
    public StoredFlow? ReadToStoredFlow(StoredId storedId, SqlDataReader reader)
    {
        while (reader.HasRows)
        {
            while (reader.Read())
            {
                var parameter = reader.IsDBNull(0) ? null : (byte[]) reader.GetValue(0);
                var status = (Status) reader.GetInt32(1);
                var result = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
                var exceptionJson = reader.IsDBNull(3) ? null : reader.GetString(3);
                var storedException = exceptionJson == null
                    ? null
                    : JsonSerializer.Deserialize<StoredException>(exceptionJson);
                var expires = reader.GetInt64(4);
                var epoch = reader.GetInt32(5);
                var interrupted = reader.GetBoolean(6);
                var timestamp = reader.GetInt64(7);
                var humanInstanceId = reader.GetString(8);
                var parentId = reader.IsDBNull(9) ? null : StoredId.Deserialize(reader.GetString(9));

                return new StoredFlow(
                    storedId,
                    humanInstanceId,
                    parameter,
                    status,
                    result,
                    storedException,
                    epoch,
                    expires,
                    timestamp,
                    interrupted,
                    parentId
                );
            }
        }

        return default;
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
                (FlowType, FlowInstance, Position, MessageJson, MessageType, IdempotencyKey)
            VALUES 
                 {messages.Select((_, i) => $"(@{prefix}FlowType{i}, @{prefix}FlowInstance{i}, @{prefix}Position{i}, @{prefix}MessageJson{i}, @{prefix}MessageType{i}, @{prefix}IdempotencyKey{i})").StringJoin($",{Environment.NewLine}")};";

        var appendCommand = StoreCommand.Create(sql);
        for (var i = 0; i < messages.Count; i++)
        {
            var (storedId, (messageContent, messageType, idempotencyKey), position) = messages[i];
            var (storedType, storedInstance) = storedId;
            appendCommand.AddParameter($"@{prefix}FlowType{i}", storedType.Value);
            appendCommand.AddParameter($"@{prefix}FlowInstance{i}", storedInstance.Value);
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
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Position >= @Position
            ORDER BY Position ASC;";

        var sql = _getMessagesSql;
        if (paramPrefix != "")
            sql = sql.Replace("@", $"@{paramPrefix}");
        var command = StoreCommand.Create(sql);
        command.AddParameter($"@{paramPrefix}FlowType", storedId.Type.Value);
        command.AddParameter($"@{paramPrefix}FlowInstance", storedId.Instance.Value);
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
}