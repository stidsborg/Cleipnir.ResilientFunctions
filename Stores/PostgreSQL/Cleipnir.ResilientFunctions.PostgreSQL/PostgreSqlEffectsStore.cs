using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlEffectsStore(string connectionString, string tablePrefix = "") : IEffectsStore
{
    private readonly PostgresCommandExecutor _commandExecutor = new(connectionString);

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_effects (
                id UUID,
                content BYTEA,
                version INT,
                PRIMARY KEY (id)
            );";
        var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        await using var cmd = new NpgsqlCommand($"TRUNCATE TABLE {tablePrefix}_effects", conn);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
    {
        if (changes.Count == 0)
            return;

        var snapshotSession = session as SnapshotStorageSession;
        if (snapshotSession == null)
        {
            var effects = (await GetEffectResults([storedId]))[storedId];
            snapshotSession = new SnapshotStorageSession(ReplicaId.Empty);
            foreach (var e in effects)
                snapshotSession.Effects[e.EffectId] = e;

            // Load version from database
            var command = StoreCommand.Create($"SELECT version FROM {tablePrefix}_effects WHERE id = $1", [storedId.AsGuid]);
            var version = (await _commandExecutor.ExecuteScalar(command)) as int?;
            snapshotSession.RowExists = version.HasValue;
            snapshotSession.Version = version ?? 0;
        }
        
        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                snapshotSession.Effects.Remove(change.EffectId);
            else
                snapshotSession.Effects[change.EffectId] = change.StoredEffect!;

        var content = snapshotSession.Serialize();
        
        if (snapshotSession.RowExists)
        {
            snapshotSession.Version++;
            await _commandExecutor.ExecuteNonQuery(
                StoreCommand.Create($"UPDATE {tablePrefix}_effects SET content = $1 WHERE id = $2", [content, storedId.AsGuid])
            );
        }
        else
        {
            snapshotSession.RowExists = true;
            await _commandExecutor.ExecuteNonQuery(
                StoreCommand.Create($"INSERT INTO {tablePrefix}_effects (id, content, version) VALUES ($1, $2, 0)", [storedId.AsGuid, content])
            );
        }
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
    {
        storedIds = storedIds.ToList();
        var sql = $@"
            SELECT id, content
            FROM {tablePrefix}_effects
            WHERE id = ANY($1)";

        var cmd = StoreCommand.Create(sql, [storedIds.Select(s => s.AsGuid).ToList()]);
        await using var reader = await _commandExecutor.Execute(cmd);

        var toReturn = new Dictionary<StoredId, List<StoredEffect>>();
        foreach (var storedId in storedIds)
            toReturn[storedId] = new List<StoredEffect>();
        
        while (await reader.ReadAsync())
        {
            var id = reader.GetGuid(0).ToStoredId();
            var content = (byte[])reader.GetValue(1);
            var effectsBytes = BinaryPacker.Split(content);
            foreach (var effectsByte in effectsBytes)
                toReturn[id].Add(StoredEffect.Deserialize(effectsByte!));
        }
        
        return toReturn;
    }
    
    public async Task Remove(StoredId storedId)
    {
        await _commandExecutor.ExecuteNonQuery(
            StoreCommand.Create($"DELETE FROM {tablePrefix}_effects WHERE id = $1", [storedId.AsGuid])
        );
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}