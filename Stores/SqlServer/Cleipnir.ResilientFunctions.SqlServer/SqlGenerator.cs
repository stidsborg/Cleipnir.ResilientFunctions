using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

internal static class SqlGenerator
{
    public static string Interrupt(IEnumerable<StoredId> storedIds, string tableName)
    {
        var conditionals = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(group => $"(FlowType = {group.Key} AND FlowInstance IN ({group.Select(i => $"'{i}'").StringJoin(", ")}))")
            .StringJoin(" OR ");

        var sql = @$"
                UPDATE {tableName}
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

        return sql;
    }
    
    public static string UpdateEffects(SqlCommand command, IReadOnlyList<StoredEffectChange> changes, string tablePrefix, string paramPrefix)
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
        for (var i = 0; i < upserts.Count; i++)
        {
            var upsert = upserts[i];
            command.Parameters.AddWithValue($"@{paramPrefix}FlowType{i}", upsert.Type);
            command.Parameters.AddWithValue($"@{paramPrefix}FlowInstance{i}", upsert.Instance);
            command.Parameters.AddWithValue($"@{paramPrefix}StoredId{i}", upsert.StoredEffectId);
            command.Parameters.AddWithValue($"@{paramPrefix}EffectId{i}", upsert.EffectId.Serialize());
            command.Parameters.AddWithValue($"@{paramPrefix}Status{i}", upsert.WorkStatus);
            command.Parameters.AddWithValue($"@{paramPrefix}Result{i}", upsert.Result ?? (object) SqlBinary.Null);
            command.Parameters.AddWithValue($"@{paramPrefix}Exception{i}", JsonHelper.ToJson(upsert.Exception) ?? (object) DBNull.Value);
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
        
        return stringBuilder.ToString();
    }
}