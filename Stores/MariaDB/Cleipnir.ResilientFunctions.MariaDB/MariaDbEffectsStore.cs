using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.MariaDB.StoreCommand;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbEffectsStore : IEffectsStore
{
    private readonly MariaDbStateStore _mariaDbStateStore;
    private readonly MariaDbCommandExecutor _commandExecutor;
    private readonly string _tablePrefix;

    public MariaDbEffectsStore(string connectionString, string tablePrefix = "")
    {
        _tablePrefix = tablePrefix == "" ? "rfunctions" : tablePrefix;
        _mariaDbStateStore = new MariaDbStateStore(connectionString, _tablePrefix);
        _commandExecutor = new MariaDbCommandExecutor(connectionString);
    }

    public async Task Initialize() => await _mariaDbStateStore.Initialize();
    public async Task Truncate()
    {
        var command = StoreCommand.Create($"UPDATE {_tablePrefix} SET effects = NULL");
        await _commandExecutor.ExecuteNonQuery(command);
    }

    public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
    {
        if (changes.Count == 0)
            return;

        var owner = default(ReplicaId);
        var existingEffects = new Dictionary<EffectId, StoredEffect>();
        var snapshotSession = session as SnapshotStorageSession;
        if (snapshotSession != null)
        {
            existingEffects = snapshotSession.Effects;
            owner = snapshotSession.ReplicaId;
        }
        else 
        {
            var effects = (await GetEffectResults([storedId]))[storedId];
            foreach (var e in effects)
                existingEffects[e.EffectId] = e;

            var ownerCmd = StoreCommand.Create(
                $"SELECT owner FROM {_tablePrefix} WHERE id = ?",
                [storedId.AsGuid.ToString("N")]
            );
            owner = (await _commandExecutor.ExecuteScalar(ownerCmd) as string)?.ToGuid().ToReplicaId();
        }

        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                existingEffects.Remove(change.EffectId);
            else
                existingEffects[change.EffectId] = change.StoredEffect!;

        var content = SnapshotStorageSession.Serialize(existingEffects);

        var command =
            owner != null
                ? StoreCommand.Create(
                    $"UPDATE {_tablePrefix} SET effects = ? WHERE id = ? AND owner = ?",
                    [
                        content,
                        storedId.AsGuid.ToString("N"),
                        owner.AsGuid.ToString("N")
                    ])
                : StoreCommand.Create(
                    $@"UPDATE {_tablePrefix} SET effects = ? WHERE id = ? AND owner IS NULL",
                    [
                        content,
                        storedId.AsGuid.ToString("N")
                    ]
                );
        
        var affectedRows = await _commandExecutor.ExecuteNonQuery(command);
        if (affectedRows == 0)
            throw UnexpectedStateException.ConcurrentModification(storedId);
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
    {
        var storedIdsList = storedIds.ToList();
        var toReturn = new Dictionary<StoredId, List<StoredEffect>>();

        if (!storedIdsList.Any())
            return toReturn;

        var sql = @$"
            SELECT id, effects
            FROM {_tablePrefix}
            WHERE id IN ({storedIdsList.Select(id => $"'{id.AsGuid:N}'").StringJoin(", ")})";

        var command = StoreCommand.Create(sql);
        await using var reader = await _commandExecutor.Execute(command);

        foreach (var storedId in storedIdsList)
            toReturn[storedId] = new List<StoredEffect>();

        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0).ToGuid().ToStoredId();
            var hasEffects = !await reader.IsDbNullAsync(1);

            if (hasEffects)
            {
                var effectsContent = (byte[])reader.GetValue(1);
                var effectsBytes = BinaryPacker.Split(effectsContent);
                var storedEffects = effectsBytes.Select(effectBytes => StoredEffect.Deserialize(effectBytes!)).ToList();
                toReturn[id] = storedEffects;
            }
        }

        return toReturn;
    }

    public async Task Remove(StoredId storedId)
    {
        var sql = $@"UPDATE {_tablePrefix} SET effects = NULL WHERE id = ?";
        var command = StoreCommand.Create(sql, [storedId.AsGuid.ToString("N")]);
        await _commandExecutor.ExecuteNonQuery(command);
    }

    private async Task<SnapshotStorageSession> CreateSnapshotStorageSession(StoredId storedId, ReplicaId owner)
    {
        var effects = (await GetEffectResults([storedId]))[storedId];
        var snapshotSession = new SnapshotStorageSession(owner);
        foreach (var e in effects)
            snapshotSession.Effects[e.EffectId] = e;

        // Load version and RowExists from database
        var command = _mariaDbStateStore.Get([storedId]);
        await using var reader = await _commandExecutor.Execute(command);
        var storedStates = await _mariaDbStateStore.Read(reader);
        if (storedStates.TryGetValue(storedId, out var states) && states.ContainsKey(0))
        {
            snapshotSession.RowExists = true;
            snapshotSession.Version = states[0].Version;
        }

        return snapshotSession;
    }
}