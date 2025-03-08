using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class SqlGenerator(string tablePrefix)
{
    public string? Interrupt(IEnumerable<StoredId> storedIds)
    {
        var conditionals = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(group => $"(type = {group.Key} AND instance IN ({group.Select(i => $"'{i}'").StringJoin(", ")}))")
            .StringJoin(" OR ");

        if (string.IsNullOrEmpty(conditionals))
            return null;
        
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
                WHERE {conditionals}";
        
        return sql;
    }
    
    public IEnumerable<NpgsqlBatchCommand> UpdateEffects(IReadOnlyList<StoredEffectChange> changes)
   {
       var commands = new List<NpgsqlBatchCommand>(changes.Count);
       var sql= $@"
         INSERT INTO {tablePrefix}_effects
             (type, instance, id_hash, status, result, exception, effect_id)
         VALUES
             ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (type, instance, id_hash)
         DO
           UPDATE SET status = EXCLUDED.status, result = EXCLUDED.result, exception = EXCLUDED.exception";
      
       foreach (var (storedId, _, _, storedEffect) in changes.Where(s => s.Operation == CrudOperation.Upsert))
       {
           var command = new NpgsqlBatchCommand(sql)
           {
               Parameters =
               {
                   new() {Value = storedId.Type.Value},
                   new() {Value = storedId.Instance.Value},
                   new() {Value = storedEffect!.StoredEffectId.Value},
                   new() {Value = (int) storedEffect.WorkStatus},
                   new() {Value = storedEffect.Result ?? (object) DBNull.Value},
                   new() {Value = JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value},
                   new() {Value = storedEffect.EffectId.Serialize()},
               }
           };
           commands.Add(command);
       }

       var removedEffects = changes
           .Where(s => s.Operation == CrudOperation.Delete)
           .Select(s => new { Id = s.StoredId, s.EffectId })
           .GroupBy(s => s.Id, s => s.EffectId.Value);

       foreach (var removedEffectGroup in removedEffects)
       {
           var storedId = removedEffectGroup.Key;
           commands.Add(
               new NpgsqlBatchCommand(
                   @$"DELETE FROM {tablePrefix}_effects
                      WHERE type = {storedId.Type.Value} AND
                            instance = '{storedId.Instance.Value}' AND
                            id_hash IN ({removedEffectGroup.Select(id => $"'{id}'").StringJoin(", ")});"
               )
           );
       }

       return commands;
   }
    
    private string? _createFunctionSql;
    public PostgresCommand CreateFunction(
        StoredId storedId,
        FlowInstance humanInstanceId,
        byte[]? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent)
    {
        _createFunctionSql ??= @$"
            INSERT INTO {tablePrefix}
                (type, instance, status, param_json, expires, timestamp, human_instance_id, parent)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT DO NOTHING;";


        var cmd = new PostgresCommand
        {
            Sql = _createFunctionSql,
            Parameters =
            [
                storedId.Type.Value,
                storedId.Instance.Value,
                (int)(postponeUntil == null ? Status.Executing : Status.Postponed),
                param == null ? DBNull.Value : param,
                postponeUntil ?? leaseExpiration,
                timestamp,
                humanInstanceId.Value,
                parent?.Serialize() ?? (object)DBNull.Value
            ]
        };
        return cmd;
    }
    
    private string? _succeedFunctionSql;
    public PostgresCommand SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch)
    {
        _succeedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Succeeded}, result_json = $1, timestamp = $2
            WHERE 
                type = $3 AND 
                instance = $4 AND 
                epoch = $5";

        return new PostgresCommand
        {
            Sql = _succeedFunctionSql,
            Parameters = [
                result == null ? DBNull.Value : result,
                timestamp,
                storedId.Type.Value,
                storedId.Instance.Value,
                expectedEpoch,
            ]
        };
    }
    
    private string? _postponeFunctionSql;
    public PostgresCommand PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        bool ignoreInterrupted,
        int expectedEpoch)
    {
        _postponeFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Postponed}, expires = $1, timestamp = $2
            WHERE 
                type = $3 AND 
                instance = $4 AND 
                epoch = $5 AND
                interrupted = FALSE";
        
        var sql = _postponeFunctionSql;
        if (ignoreInterrupted)
            sql = sql.Replace("interrupted = FALSE", "1 = 1");
        
        return new PostgresCommand
        {
            Sql = sql,
            Parameters =
            [
                postponeUntil,
                timestamp,
                storedId.Type.Value,
                storedId.Instance.Value,
                expectedEpoch,
            ]
        };
    }
    
    private string? _failFunctionSql;
    public PostgresCommand FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch)
    {
        _failFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Failed}, exception_json = $1, timestamp = $2
            WHERE 
                type = $3 AND 
                instance = $4 AND 
                epoch = $5";
        return new PostgresCommand
        {
            Sql = _failFunctionSql,
            Parameters = [
                JsonSerializer.Serialize(storedException),
                timestamp,
                storedId.Type.Value,
                storedId.Instance.Value,
                expectedEpoch,
            ]
        };
    }
}