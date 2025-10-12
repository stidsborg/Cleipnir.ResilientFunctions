using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlEffectsStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "") : IEffectsStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_effects (
                id UUID,
                id_hash UUID,
                status INT NOT NULL,
                result BYTEA NULL,
                exception TEXT NULL,
                effect_id TEXT NOT NULL,
                PRIMARY KEY (id, id_hash)
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
    public async Task SetEffectResult(StoredId storedId, StoredEffect storedEffect, IStorageSession? session)
    {
        await using var conn = await CreateConnection();
        _setEffectResultSql ??= $@"
          INSERT INTO {tablePrefix}_effects
              (id, id_hash, status, result, exception, effect_id)
          VALUES
              ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (id, id_hash)
          DO
            UPDATE SET status = EXCLUDED.status, result = EXCLUDED.result, exception = EXCLUDED.exception";

        await using var command = new NpgsqlCommand(_setEffectResultSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.AsGuid},
                new() {Value = storedEffect.StoredEffectId.Value},
                new() {Value = (int) storedEffect.WorkStatus},
                new() {Value = storedEffect.Result ?? (object) DBNull.Value},
                new() {Value = JsonHelper.ToJson(storedEffect.StoredException) ?? (object) DBNull.Value},
                new() {Value = storedEffect.EffectId.Serialize()},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
    {
        if (changes.Count == 0)
            return;

        await using var batch = sqlGenerator.UpdateEffects(changes).ToNpgsqlBatch();
        await using var conn = await CreateConnection();
        batch.WithConnection(conn);

        await batch.ExecuteNonQueryAsync();
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
    {
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetEffects(storedIds).ToNpgsqlCommand(conn);

        await using var reader = await command.ExecuteReaderAsync();
        var effects = await sqlGenerator.ReadEffectsForIds(reader, storedIds);
        return effects;
    }
    
    private string? _removeSql;
    public async Task Remove(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= $"DELETE FROM {tablePrefix}_effects WHERE id = $1";
        
        await using var command = new NpgsqlCommand(_removeSql, conn)
        {
            Parameters =
            {
                new() {Value = storedId.AsGuid },
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