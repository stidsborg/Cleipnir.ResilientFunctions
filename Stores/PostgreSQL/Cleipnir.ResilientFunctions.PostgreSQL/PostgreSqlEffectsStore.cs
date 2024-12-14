using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlEffectsStore(string connectionString, string tablePrefix = "") : IEffectsStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_effects (
                type INT,
                instance UUID,
                id_hash UUID,
                is_state BOOLEAN,
                status INT NOT NULL,
                result BYTEA NULL,
                exception TEXT NULL,
                effect_id TEXT NOT NULL,
                PRIMARY KEY (type, instance, id_hash, is_state)
            );";
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_effects";
        var command = new NpgsqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _setEffectResultSql;
    public async Task SetEffectResult(StoredId storedId, StoredEffect storedEffect)
    {
        await using var conn = await CreateConnection();
        _setEffectResultSql ??= $@"
          INSERT INTO {tablePrefix}_effects 
              (type, instance, id_hash, is_state, status, result, exception, effect_id)
          VALUES
              ($1, $2, $3, $4, $5, $6, $7, $8) 
          ON CONFLICT (type, instance, id_hash, is_state) 
          DO 
            UPDATE SET status = EXCLUDED.status, result = EXCLUDED.result, exception = EXCLUDED.exception";
        
        await using var command = new NpgsqlCommand(_setEffectResultSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value},
                new() {Value = storedId.Instance.Value},
                new() {Value = storedEffect.StoredEffectId.Value},
                new() {Value = storedEffect.IsState},
                new() {Value = (int) storedEffect.WorkStatus},
                new() {Value = storedEffect.Result ?? (object) DBNull.Value},
                new() {Value = JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value},
                new() {Value = storedEffect.EffectId.Value},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffect> storedEffects)
    {
        await using var conn = await CreateConnection();
        _setEffectResultSql ??= $@"
          INSERT INTO {tablePrefix}_effects 
              (type, instance, id_hash, is_state, status, result, exception, effect_id)
          VALUES
              ($1, $2, $3, $4, $5, $6, $7, $8) 
          ON CONFLICT (type, instance, id_hash, is_state) 
          DO 
            UPDATE SET status = EXCLUDED.status, result = EXCLUDED.result, exception = EXCLUDED.exception";
        
        await using var batch = new NpgsqlBatch(conn);
        foreach (var storedEffect in storedEffects)
        {
            var command = new NpgsqlBatchCommand(_setEffectResultSql)
            {
                Parameters =
                {
                    new() {Value = storedId.Type.Value},
                    new() {Value = storedId.Instance.Value},
                    new() {Value = storedEffect.StoredEffectId.Value},
                    new() {Value = storedEffect.IsState},
                    new() {Value = (int) storedEffect.WorkStatus},
                    new() {Value = storedEffect.Result ?? (object) DBNull.Value},
                    new() {Value = JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value},
                    new() {Value = storedEffect.EffectId.Value},
                }
            };
            batch.BatchCommands.Add(command);
        }
       

        await batch.ExecuteNonQueryAsync();
    }

    private string? _getEffectResultsSql;
    public async Task<IReadOnlyList<StoredEffect>> GetEffectResults(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _getEffectResultsSql ??= @$"
            SELECT id_hash, is_state, status, result, exception, effect_id
            FROM {tablePrefix}_effects
            WHERE type = $1 AND instance = $2;";
        await using var command = new NpgsqlCommand(_getEffectResultsSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.Type.Value },
                new() { Value = storedId.Instance.Value },
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredEffect>();
        while (await reader.ReadAsync())
        {
            var idHash = reader.GetGuid(0);
            var isState = reader.GetBoolean(1);
            var status = (WorkStatus) reader.GetInt32(2);
            var result = reader.IsDBNull(3) ? null : (byte[]) reader.GetValue(3);
            var exception = reader.IsDBNull(4) ? null : reader.GetString(4);
            var effectId = reader.GetString(5);
            functions.Add(
                new StoredEffect(effectId, new StoredEffectId(idHash), isState, status, result, JsonHelper.FromJson<StoredException>(exception))
            );
        }

        return functions;
    }

    private string? _deleteEffectResultSql;
    public async Task DeleteEffectResult(StoredId storedId, StoredEffectId effectId, bool isState)
    {
        await using var conn = await CreateConnection();
        _deleteEffectResultSql ??= $"DELETE FROM {tablePrefix}_effects WHERE type = $1 AND instance = $2 AND id_hash = $3 AND is_state = $4";
        
        await using var command = new NpgsqlCommand(_deleteEffectResultSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value },
                new() {Value = storedId.Instance.Value },
                new() {Value = effectId.Value },
                new() {Value = isState },
            }
        };

        await command.ExecuteNonQueryAsync();
    }
    
    private string? _removeSql;
    public async Task Remove(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= $"DELETE FROM {tablePrefix}_effects WHERE type = $1 AND instance = $2";
        
        await using var command = new NpgsqlCommand(_removeSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.Type.Value },
                new() {Value = storedId.Instance.Value },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}