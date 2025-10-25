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
                position BIGINT,               
                status INT NOT NULL,
                result LONGBLOB NULL,
                exception TEXT NULL,
                effect_id TEXT NOT NULL,
                PRIMARY KEY(id, position)
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

        var positionsSession = session as PositionsStorageSession ?? await CreateSession(storedId);
        await using var conn = await CreateConnection();
        await using var command = sqlGenerator
            .UpdateEffects(storedId, changes, positionsSession)
            ?.ToSqlCommand(conn);

        if (command != null)
            await command.ExecuteNonQueryAsync();
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
        => (await GetEffectResultsWithPositions(storedIds))
            .ToDictionary(kv => kv.Key, kv => kv.Value.Select(s => s.Effect).ToList());

    private async Task<Dictionary<StoredId, List<StoredEffectWithPosition>>> GetEffectResultsWithPositions(IEnumerable<StoredId> storedIds)
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

    private async Task<PositionsStorageSession> CreateSession(StoredId storedId)
        => await CreateSessions([storedId]).SelectAsync(d => d[storedId]);
    private async Task<Dictionary<StoredId, PositionsStorageSession>> CreateSessions(IEnumerable<StoredId> storedIds)
        => CreateSessions(await GetEffectResultsWithPositions(storedIds));

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