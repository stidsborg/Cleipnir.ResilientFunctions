using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.MariaDB.StoreCommand;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbEffectsStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "") : IEffectsStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_effects (
                id CHAR(32),
                position INT,
                content LONGBLOB,
                version INT,
                PRIMARY KEY (id, position)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_effects";
        var command = new MySqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
    {
        if (changes.Count == 0)
            return;

        var storageSession = session as SnapshotStorageSession ?? await CreateSession(storedId);
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator
            .UpdateEffects(storedId, changes, storageSession)
            .ToSqlCommand(conn);

        await command.ExecuteNonQueryAsync();
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
        => (await GetEffectResultsWithSession(storedIds)).ToDictionary(kv => kv.Key, kv => kv.Value.Effects.Values.ToList());

    public async Task<Dictionary<StoredId, SnapshotStorageSession>> GetEffectResultsWithSession(IEnumerable<StoredId> storedIds)
    {
        storedIds = storedIds.ToList();
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator.GetEffects(storedIds).ToSqlCommand(conn);
        await using var reader = await command.ExecuteReaderAsync();

        var effects = await sqlGenerator.ReadEffectsForMultipleStoredIds(reader, storedIds);
        return effects;
    }
    
    private string? _removeSql;
    public async Task Remove(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _removeSql ??= $"DELETE FROM {tablePrefix}_effects WHERE id = ?";
        await using var command = new MySqlCommand(_removeSql, conn)
        {
            Parameters =
            {
                new() { Value = storedId.AsGuid.ToString("N") },
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }

    private async Task<SnapshotStorageSession> CreateSession(StoredId storedId)
        => await CreateSessions([storedId]).SelectAsync(d => d[storedId]);

    private async Task<Dictionary<StoredId, SnapshotStorageSession>> CreateSessions(IEnumerable<StoredId> storedIds)
        => await GetEffectResultsWithSession(storedIds);
}