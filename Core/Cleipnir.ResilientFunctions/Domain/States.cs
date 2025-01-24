using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class States(
    StoredId storedId,
    IEffectsStore effectStore,
    Lazy<Task<IReadOnlyList<StoredEffect>>> lazyEffects,
    ISerializer serializer)
{
    private Dictionary<StateId, StoredState>? _existingStoredStates;
    private readonly Dictionary<StateId, FlowState> _existingStates = new();
    
    private readonly Lock _sync = new();

    private async Task<Dictionary<StateId, StoredState>> GetExistingStoredStates()
    {
        lock (_sync)
            if (_existingStoredStates is not null) 
                return _existingStoredStates;

        var existingStatesDict = (await lazyEffects.Value)
            .Where(se => se.EffectId.Type.IsState())
            .ToDictionary(se => new StateId(se.EffectId.Id), se => new StoredState(se.EffectId.Id, se.Result!));
        
        lock (_sync) 
            return _existingStoredStates ??= existingStatesDict;
    }

    public Task<T> CreateOrGetDefault<T>() where T : FlowState, new()
        => CreateOrGetInner<T>(id: "");
    
    public async Task<T> CreateOrGet<T>(string id) where T : FlowState, new()
    {
        if (string.IsNullOrEmpty(id))
            throw new ArgumentException("Id must not be empty string or null", nameof(id));

        return await CreateOrGetInner<T>(id);
    }
    
    private async Task<T> CreateOrGetInner<T>(string id) where T : FlowState, new()
    {
        var existingStoredStates = await GetExistingStoredStates();
        
        lock (_sync)
            if (_existingStates.TryGetValue(id, out var state))
                return (T)state;
            else if (existingStoredStates.TryGetValue(key: id, out var storedState))
            {
                var s = serializer.DeserializeState<T>(storedState.StateJson);
                _existingStates[id] = s;
                s.Initialize(onSave: () => SaveState(id, s));
                return s;
            }
            else
            {
                var newState = new T();
                newState.Initialize(onSave: () => SaveState(id, newState));
                _existingStates[id] = newState;
                existingStoredStates[id] = new StoredState(id, serializer.SerializeState(newState));
                return newState;
            }
    }

    public Task RemoveDefault() => RemoveInner(id: "");
    
    public async Task Remove(string id)
    {
        if (string.IsNullOrEmpty(id))
            throw new ArgumentException("Id must not be empty string or null", nameof(id));

        await RemoveInner(id);
    }
    
    private async Task RemoveInner(string id)
    {
        var existingStoredStates = await GetExistingStoredStates(); 
        
        lock (_sync)
            if (!existingStoredStates.ContainsKey(id))
                return;
        
        await effectStore.DeleteEffectResult(storedId, id.ToStoredEffectId(effectType: EffectType.State));

        lock (_sync)
        {
            existingStoredStates.Remove(id);
            _existingStates.Remove(id);
        }
    }

    private async Task SaveState<T>(string id, T state) where T : FlowState, new()
    {
        var json = serializer.SerializeState(state);
        var storedState = new StoredState(new StateId(id), json);
        var storedEffect = StoredEffect.CreateState(storedState);
        await effectStore.SetEffectResult(storedId, storedEffect);
    }
}