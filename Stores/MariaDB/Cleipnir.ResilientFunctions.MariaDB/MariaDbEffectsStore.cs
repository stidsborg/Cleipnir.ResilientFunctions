using Cleipnir.ResilientFunctions.Domain;
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

    public MariaDbEffectsStore(string connectionString, string tablePrefix = "")
    {
        _mariaDbStateStore = new MariaDbStateStore(connectionString, tablePrefix);
        _commandExecutor = new MariaDbCommandExecutor(connectionString);
    }

    public async Task Initialize() => await _mariaDbStateStore.Initialize();
    public async Task Truncate() => await _mariaDbStateStore.Truncate();

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

            // Load version and RowExists from database
            var command = _mariaDbStateStore.Get([storedId]);
            await using var reader = await _commandExecutor.Execute(command);
            var storedStates = await _mariaDbStateStore.Read(reader);
            if (storedStates.TryGetValue(storedId, out var states) && states.ContainsKey(0))
            {
                snapshotSession.RowExists = true;
                snapshotSession.Version = states[0].Version;
            }
        }
        
        foreach (var change in changes)
            if (change.Operation == CrudOperation.Delete)
                snapshotSession.Effects.Remove(change.EffectId);
            else
                snapshotSession.Effects[change.EffectId] = change.StoredEffect!;

        var content = snapshotSession.Serialize();

        var storedState = new MariaDbStateStore.StoredState(
            storedId,
            Position: 0,
            content,
            snapshotSession.Version
        );

        if (snapshotSession.RowExists)
        {
            snapshotSession.Version++;
            await _commandExecutor.ExecuteNonQuery(_mariaDbStateStore.Update(storedId, storedState));
        }
        else
        {
            snapshotSession.RowExists = true;
            await _commandExecutor.ExecuteNonQuery(_mariaDbStateStore.Insert(storedId, storedState));
        }
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
    {
        storedIds = storedIds.ToList();
        var command = _mariaDbStateStore.Get(storedIds.ToList());
        await using var reader = await _commandExecutor.Execute(command);
        var storedStates = await _mariaDbStateStore.Read(reader);
        var toReturn = new Dictionary<StoredId, List<StoredEffect>>();
        foreach (var storedId in storedIds)
            toReturn[storedId] = new List<StoredEffect>();

        foreach (var (id, states) in storedStates)
        {
            var storedState = states[0];
            var effectsBytes = BinaryPacker.Split(storedState.Content!);
            var storedEffects = effectsBytes.Select(effectBytes => StoredEffect.Deserialize(effectBytes!)).ToList();
            toReturn[id] = storedEffects;
        }

        return toReturn;
    }
    
    public async Task Remove(StoredId storedId)
        => await _commandExecutor.ExecuteNonQuery(_mariaDbStateStore.Delete(storedId));
}