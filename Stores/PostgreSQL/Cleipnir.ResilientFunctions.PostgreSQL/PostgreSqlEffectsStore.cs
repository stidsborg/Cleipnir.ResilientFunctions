﻿using System;
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
                id VARCHAR(450) PRIMARY KEY,
                status INT NOT NULL,
                result TEXT NULL,
                exception TEXT NULL
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
    public async Task SetEffectResult(FlowId flowId, StoredEffect storedEffect)
    {
        var (flowType, flowInstance) = flowId;
        
        await using var conn = await CreateConnection();
        _setEffectResultSql ??= $@"
          INSERT INTO {tablePrefix}_effects 
              (id, status, result, exception)
          VALUES
              ($1, $2, $3, $4) 
          ON CONFLICT (id) 
          DO 
            UPDATE SET status = EXCLUDED.status, result = EXCLUDED.result, exception = EXCLUDED.exception";
        
        await using var command = new NpgsqlCommand(_setEffectResultSql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(flowType.Value, flowInstance.Value, storedEffect.EffectId.Value)},
                new() {Value = (int) storedEffect.WorkStatus},
                new() {Value = storedEffect.Result ?? (object) DBNull.Value},
                new() {Value = JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _getEffectResultsSql;
    public async Task<IEnumerable<StoredEffect>> GetEffectResults(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _getEffectResultsSql ??= @$"
            SELECT id, status, result, exception
            FROM {tablePrefix}_effects
            WHERE id LIKE $1";
        await using var command = new NpgsqlCommand(_getEffectResultsSql, conn)
        {
            Parameters =
            {
                new() { Value = Escaper.Escape(flowId.Type.Value, flowId.Instance.Value) + $"{Escaper.Separator}%" }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredEffect>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0);
            var effectId = Escaper.Unescape(id)[2];
            var status = (WorkStatus) reader.GetInt32(1);
            var result = reader.IsDBNull(2) ? null : reader.GetString(2);
            var exception = reader.IsDBNull(3) ? null : reader.GetString(3);
            functions.Add(new StoredEffect(effectId, status, result, JsonHelper.FromJson<StoredException>(exception)));
        }

        return functions;
    }

    private string? _deleteEffectResultSql;
    public async Task DeleteEffectResult(FlowId flowId, EffectId effectId)
    {
        await using var conn = await CreateConnection();
        _deleteEffectResultSql ??= $"DELETE FROM {tablePrefix}_effects WHERE id = $1";
        
        var id = Escaper.Escape(flowId.Type.Value, flowId.Instance.Value, effectId.Value);
        await using var command = new NpgsqlCommand(_deleteEffectResultSql, conn)
        {
            Parameters = { new() {Value = id } }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _removeSql;
    public async Task Remove(FlowId flowId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= $"DELETE FROM {tablePrefix}_effects WHERE id LIKE $1";
        
        var id = Escaper.Escape(flowId.Type.Value, flowId.Instance.Value) + $"{Escaper.Separator}%";
        await using var command = new NpgsqlCommand(_removeSql, conn)
        {
            Parameters = { new() {Value = id } }
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