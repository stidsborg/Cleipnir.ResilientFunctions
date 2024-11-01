using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingStates
{
    private readonly StoredId _storedId;
    private Dictionary<StateId, StoredState>? _storedStates;
    private readonly IFunctionStore _functionStore;
    private readonly IEffectsStore _effectsStore;
    private readonly ISerializer _serializer;

    public ExistingStates(StoredId storedId, IFunctionStore functionStore, ISerializer serializer)
    {
        _storedId = storedId;
        _functionStore = functionStore;
        _effectsStore = functionStore.EffectsStore;
        _serializer = serializer;
    }

    private async Task<Dictionary<StateId, StoredState>> GetStoredStates()
    {
        if (_storedStates is not null)
            return _storedStates;

        return _storedStates = (await _functionStore.EffectsStore.GetEffectResults(_storedId))
            .Where(se => se.IsState)
            .Select(se => new StoredState(se.EffectId.Value, se.Result!))
            .ToDictionary(s => s.StateId, s => s);
    }
    
    public Task<IEnumerable<StateId>> StateIds => GetStoredStates().ContinueWith(t => t.Result.Keys.AsEnumerable());
    public Task<bool> HasState(string stateId) => GetStoredStates().ContinueWith(t => t.Result.ContainsKey(stateId));
    public Task<bool> HasDefaultState() => HasState(stateId: "");

    public Task<TState> Get<TState>() where TState : FlowState, new()
        => Get<TState>(stateId: "");
    
    public async Task<TState> Get<TState>(string stateId) where TState : FlowState, new()
    {
        var storedStates = await GetStoredStates();
        
        if (!storedStates.TryGetValue(stateId, out var storedState))
            throw new KeyNotFoundException($"State '{stateId}' was not found");

        var state = _serializer.DeserializeState<TState>(storedState.StateJson);
        state.Initialize(onSave: () => Set(stateId, state));
        return state;
    }

    public Task RemoveDefault() => Remove(stateId: ""); 
    public async Task Remove(string stateId)
    {
        var storedStates = await GetStoredStates();
        await _effectsStore.DeleteEffectResult(_storedId, stateId, isState: true);
        storedStates.Remove(stateId);
    }

    public Task Set<TState>(TState state) where TState : FlowState, new()
        => Set(stateId: "", state);
    
    public async Task Set<TState>(string stateId, TState state) where TState : FlowState, new()
    {
        var storedStates = await GetStoredStates();
        var json = _serializer.SerializeState(state);
        var storedState = new StoredState(stateId, json);
        await _effectsStore.SetEffectResult(_storedId, StoredEffect.CreateState(storedState));
        storedStates[stateId] = storedState;
    }
}