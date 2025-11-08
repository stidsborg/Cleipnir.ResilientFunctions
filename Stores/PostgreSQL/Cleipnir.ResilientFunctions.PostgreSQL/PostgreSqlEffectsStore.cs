using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgreSqlEffectsStore(string connectionString, string tablePrefix = "") : IEffectsStore
{
    private readonly PostgreSqlStateStore _stateStore = new(connectionString, tablePrefix);
    private readonly PostgresCommandExecutor _commandExecutor = new(connectionString);

    public async Task Initialize() => await _stateStore.Initialize();
    public async Task Truncate() => await _stateStore.Truncate();

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
            var command = _stateStore.Get([storedId]);
            await using var reader = await _commandExecutor.Execute(command);
            var storedStates = await _stateStore.Read(reader);
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
        
        var storedState = new PostgreSqlStateStore.StoredState(
            storedId,
            Position: 0,
            content,
            snapshotSession.Version
        );

        if (snapshotSession.RowExists)
        {
            snapshotSession.Version++;
            await _commandExecutor.ExecuteNonQuery(_stateStore.Update(storedId, storedState));            
        }
        else
        {
            snapshotSession.RowExists = true;            
            await _commandExecutor.ExecuteNonQuery(_stateStore.Insert(storedId, storedState));
        }
    }

    public async Task<Dictionary<StoredId, List<StoredEffect>>> GetEffectResults(IEnumerable<StoredId> storedIds)
    {
        storedIds = storedIds.ToList();
        var command = _stateStore.Get(storedIds.ToList());
        await using var reader = await _commandExecutor.Execute(command);
        var storedStates = await _stateStore.Read(reader);
        var toReturn = new Dictionary<StoredId, List<StoredEffect>>();
        foreach (var storedId in storedIds)
            toReturn[storedId] = new List<StoredEffect>();
        
        foreach (var (id, states) in storedStates)
        {
            var storedState = states[0];
            var effectsBytes = BinaryPacker.Split(storedState.Content!);
            var storedEffects = effectsBytes.Select(effectBytes => StoredEffect.Deserialize(effectBytes!)).ToList();
            toReturn[id] =  storedEffects;
        }

        return toReturn;
    }
    
    public async Task Remove(StoredId storedId)
    {
        var cmd = _stateStore.Delete(storedId);
        await _commandExecutor.ExecuteNonQuery(cmd);
    }
}