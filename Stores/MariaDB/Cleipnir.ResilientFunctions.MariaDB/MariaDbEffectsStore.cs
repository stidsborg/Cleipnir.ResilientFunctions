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

    public MariaDbEffectsStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "")
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
            snapshotSession = await CreateSession(storedId);

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
        => (await GetEffectResultsWithSession(storedIds)).ToDictionary(kv => kv.Key, kv => kv.Value.Effects.Values.ToList());

    public async Task<Dictionary<StoredId, SnapshotStorageSession>> GetEffectResultsWithSession(IEnumerable<StoredId> storedIds)
    {
        storedIds = storedIds.ToList();
        var command = _mariaDbStateStore.Get(storedIds.ToList());
        await using var reader = await _commandExecutor.Execute(command);
        var storedStates = await _mariaDbStateStore.Read(reader);
        var toReturn = new Dictionary<StoredId, SnapshotStorageSession>();
        foreach (var storedId in storedIds)
            toReturn[storedId] = new SnapshotStorageSession();

        foreach (var (id, states) in storedStates)
        {
            var storedState = states[0];

            var session = toReturn[id];
            session.RowExists = true;
            session.Version = storedState.Version;

            var effectsBytes = BinaryPacker.Split(storedState.Content!);
            var storedEffects = effectsBytes.Select(effectBytes => StoredEffect.Deserialize(effectBytes!)).ToList();
            foreach (var storedEffect in storedEffects)
                session.Effects[storedEffect.EffectId] = storedEffect;
        }

        return toReturn;
    }
    
    public async Task Remove(StoredId storedId)
    {
        var cmd = _mariaDbStateStore.Delete(storedId);
        await _commandExecutor.ExecuteNonQuery(cmd);
    }

    private async Task<SnapshotStorageSession> CreateSession(StoredId storedId)
        => await CreateSessions([storedId]).SelectAsync(d => d[storedId]);

    private async Task<Dictionary<StoredId, SnapshotStorageSession>> CreateSessions(IEnumerable<StoredId> storedIds)
        => await GetEffectResultsWithSession(storedIds);
}