using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingStates
{
    private readonly FlowId _flowId;
    private Dictionary<StateId, StoredState>? _storedStates;
    private readonly IFunctionStore _functionStore;
    private readonly IStatesStore _statesStore;
    private readonly ISerializer _serializer;
    
    private string? _defaultStateJson;
    private FlowState? _defaultState;

    public ExistingStates(FlowId flowId, string? defaultState, IFunctionStore functionStore, ISerializer serializer)
    {
        _flowId = flowId;
        _functionStore = functionStore;
        _statesStore = functionStore.StatesStore;
        _serializer = serializer;

        _defaultStateJson = defaultState;
    }

    private async Task<Dictionary<StateId, StoredState>> GetStoredStates()
    {
        if (_storedStates is not null)
            return _storedStates;

        return _storedStates = (await _functionStore.StatesStore.GetStates(_flowId))
            .ToDictionary(s => s.StateId, s => s);
    }
    
    public Task<IEnumerable<StateId>> StateIds => GetStoredStates().ContinueWith(t => t.Result.Keys.AsEnumerable());
    public Task<bool> HasState(string stateId) => GetStoredStates().ContinueWith(t => t.Result.ContainsKey(stateId));
    public bool HasDefaultState() => _defaultStateJson != null;

    public TState Get<TState>() where TState : FlowState, new()
    {
        if (_defaultState != null)
            return (TState) _defaultState;
        if (_defaultStateJson != null)
            return (TState) (_defaultState = _serializer.DeserializeState<TState>(_defaultStateJson));

        var state = new TState();
        state.Initialize(() => _functionStore.SetDefaultState(_flowId, _serializer.SerializeState(state)));
        _defaultState = state;
        return state;
    }
    
    public async Task<TState> Get<TState>(string stateId) where TState : FlowState, new()
    {
        var storedStates = await GetStoredStates();
        
        if (!storedStates.TryGetValue(stateId, out var storedState))
            throw new KeyNotFoundException($"State '{stateId}' was not found");

        var state = _serializer.DeserializeState<TState>(storedState.StateJson);
        state.Initialize(onSave: () => Set(stateId, state));
        return state;
    }

    public async Task RemoveDefault()
    {
        await _functionStore.SetDefaultState(_flowId, stateJson: null);
        _defaultState = null;
        _defaultStateJson = null;
    }
    public async Task Remove(string stateId)
    {
        var storedStates = await GetStoredStates();
        await _statesStore.RemoveState(_flowId, stateId);
        storedStates.Remove(stateId);
    }

    public async Task Set<TState>(TState state) where TState : FlowState, new()
    {
        await _functionStore.SetDefaultState(_flowId, _serializer.SerializeState(state));
        _defaultState = state;
    }
    
    public async Task Set<TState>(string stateId, TState state) where TState : FlowState, new()
    {
        var storedStates = await GetStoredStates();
        var json = _serializer.SerializeState(state);
        var storedState = new StoredState(stateId, json);
        await _statesStore.UpsertState(_flowId, storedState);
        storedStates[stateId] = storedState;
    }
}