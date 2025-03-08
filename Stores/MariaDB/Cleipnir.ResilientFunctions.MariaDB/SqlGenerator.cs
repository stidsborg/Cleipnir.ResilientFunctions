using System.Text;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class SqlGenerator(string tablePrefix)
{
    public StoreCommand? Interrupt(IEnumerable<StoredId> storedIds)
    {
        var conditionals = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(group => $"(type = {group.Key} AND instance IN ({group.Select(i => $"'{i:N}'").StringJoin(", ")}))")
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

        return new StoreCommand(sql);
    }
    
    public string UpdateEffects(MySqlCommand command, IReadOnlyList<StoredEffectChange> changes)
    {
        var stringBuilder = new StringBuilder(capacity: 2);
        var upserts = changes
            .Where(c => c.Operation == CrudOperation.Upsert)
            .Select(c => new
            {
                Type = c.StoredId.Type.Value, 
                Instance = c.StoredId.Instance.Value, 
                IdHash = c.EffectId.Value,
                WorkStatus = (int)c.StoredEffect!.WorkStatus, 
                Result = c.StoredEffect!.Result,
                Exception = c.StoredEffect!.StoredException, 
                EffectId = c.StoredEffect!.EffectId
            })
            .ToList();
        
        var setSql = $@"
          INSERT INTO {tablePrefix}_effects 
              (type, instance, id_hash, status, result, exception, effect_id)
          VALUES
              {"(?, ?, ?, ?, ?, ?, ?)".Replicate(upserts.Count).StringJoin(", ")}  
           ON DUPLICATE KEY UPDATE
                status = VALUES(status), result = VALUES(result), exception = VALUES(exception);";
        if (upserts.Any())
            stringBuilder.AppendLine(setSql);
        foreach (var a in upserts)
        {
            command.Parameters.Add(new MySqlParameter(name: null, a.Type));
            command.Parameters.Add(new MySqlParameter(name: null, a.Instance.ToString("N")));
            command.Parameters.Add(new MySqlParameter(name: null, a.IdHash.ToString("N")));
            command.Parameters.Add(new MySqlParameter(name: null, a.WorkStatus));
            command.Parameters.Add(new MySqlParameter(name: null, a.Result ?? (object) DBNull.Value));
            command.Parameters.Add(new MySqlParameter(name: null, JsonHelper.ToJson(a.Exception) ?? (object) DBNull.Value));
            command.Parameters.Add(new MySqlParameter(name: null, a.EffectId.Serialize()));
        }

        var removes = changes
            .Where(c => c.Operation == CrudOperation.Delete)
            .Select(c => new { Type = c.StoredId.Type.Value, Instance = c.StoredId.Instance.Value, IdHash = c.EffectId.Value })
            .GroupBy(a => new {a.Type, a.Instance }, a => a.IdHash)
            .ToList();
        var predicates = removes
            .Select(r =>
                $"(type = {r.Key.Type} AND instance = '{r.Key.Instance:N}' AND id_hash IN ({r.Select(id => $"'{id:N}'").StringJoin(", ")}))")
            .StringJoin($" OR {Environment.NewLine}");
        var removeSql = @$"
            DELETE FROM {tablePrefix}_effects 
            WHERE {predicates}";
        if (removes.Any())
            stringBuilder.AppendLine(removeSql);
        
        return stringBuilder.ToString();
    }
    
    private string? _createFunctionSql;
    public StoreCommand CreateFunction(
        StoredId storedId, 
        FlowInstance humanInstanceId,
        byte[]? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent)
    {
        _createFunctionSql ??= @$"
            INSERT IGNORE INTO {tablePrefix}
                (type, instance, param_json, status, epoch, expires, timestamp, human_instance_id, parent)
            VALUES
                (?, ?, ?, ?, 0, ?, ?, ?, ?)";
        var status = postponeUntil == null ? Status.Executing : Status.Postponed;

        return new StoreCommand(
            _createFunctionSql,
            [
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                param ?? (object)DBNull.Value,
                (int)status,
                postponeUntil ?? leaseExpiration,
                timestamp,
                humanInstanceId.Value,
                parent?.Serialize() ?? (object)DBNull.Value,
            ]
        );
    }
    
    private string? _succeedFunctionSql;
    public StoreCommand SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch)
    {
        _succeedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Succeeded}, result_json = ?, timestamp = ?, epoch = ?
            WHERE 
                type = ? AND 
                instance = ? AND 
                epoch = ?";

        return new StoreCommand(
            _succeedFunctionSql,
            [
                result ?? (object)DBNull.Value,
                timestamp,
                expectedEpoch,
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                expectedEpoch,
            ]
        );
    }
    
    private string? _postponedFunctionSql;
    public StoreCommand PostponeFunction(
        StoredId storedId, 
        long postponeUntil, 
        long timestamp,
        bool ignoreInterrupted,
        int expectedEpoch)
    {
        _postponedFunctionSql ??= $@"
            UPDATE {tablePrefix}
            SET status = {(int) Status.Postponed}, expires = ?, timestamp = ?, epoch = ?
            WHERE 
                type = ? AND 
                instance = ? AND 
                epoch = ? AND
                interrupted = 0";

        var sql = _postponedFunctionSql;
        if (ignoreInterrupted)
            sql = sql.Replace("interrupted = 0", "1 = 1");

        return new StoreCommand(
            sql,
            [
                postponeUntil,
                timestamp,
                expectedEpoch,
                storedId.Type.Value,
                storedId.Instance.Value.ToString("N"),
                expectedEpoch,
            ]
        );
    }
}