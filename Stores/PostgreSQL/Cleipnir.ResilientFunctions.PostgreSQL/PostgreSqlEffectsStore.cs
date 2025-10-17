using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
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
                position BIGINT,
                status INT NOT NULL,
                result BYTEA NULL,
                exception TEXT NULL,
                effect_id TEXT NOT NULL,
                PRIMARY KEY (id, position)
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

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
    {
        if (changes.Count == 0)
            return;

        var positionsSession = session as PositionsStorageSession;
        await using var batch = sqlGenerator.UpdateEffects(changes, positionsSession).ToNpgsqlBatch();
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
        return effects.ToDictionary(kv => kv.Key, kv => kv.Value.Select(s => s.Effect).ToList());
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
    
    private async Task<PositionsStorageSession> CreateSession(StoredId storedId)
        => await CreateSessions([storedId]).SelectAsync(d => d[storedId]);
    private async Task<Dictionary<StoredId, PositionsStorageSession>> CreateSessions(IEnumerable<StoredId> storedIds)
    {
        sqlGenerator.GetEffects(storedIds);
        
    }

    private Dictionary<StoredId, PositionsStorageSession> CreateSessions(Dictionary<StoredId, List<StoredEffectWithPosition>> effects)
    {
        var dictionary = new Dictionary<StoredId, PositionsStorageSession>();
        foreach (var storedId in effects.Keys)
        {
            var session = new PositionsStorageSession();
            dictionary[storedId] = session;
            foreach (var (effect, position) in effects[storedId].OrderBy(e => e.Position))
            {
                session.MaxPosition = position;
                session.Positions[effect.EffectId.Serialize()] = position;
            }
        }

        return dictionary;
    }
}