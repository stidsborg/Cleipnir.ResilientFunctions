using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
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
                position INT,
                content BYTEA,
                version INT,
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

        var storageSession = session as SnapshotStorageSession ?? await CreateSession(storedId);
        await using var conn = await CreateConnection();
        await using var cmd = sqlGenerator.UpdateEffects(storedId, changes, storageSession).ToNpgsqlCommand(conn);
        
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
        => (await GetEffectResultsWithSession(storedIds)).ToDictionary(kv => kv.Key, kv => kv.Value.Effects.Values.ToList());
    
    public async Task<Dictionary<StoredId, SnapshotStorageSession>> GetEffectResultsWithSession(IEnumerable<StoredId> storedIds)
    {
        storedIds = storedIds.ToList();
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
    
    private async Task<SnapshotStorageSession> CreateSession(StoredId storedId)
        => await CreateSessions([storedId]).SelectAsync(d => d[storedId]);

    private async Task<Dictionary<StoredId, SnapshotStorageSession>> CreateSessions(IEnumerable<StoredId> storedIds) 
        => await GetEffectResultsWithSession(storedIds);
}