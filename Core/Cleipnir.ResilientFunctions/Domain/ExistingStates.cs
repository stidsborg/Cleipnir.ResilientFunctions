using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class ExistingStates
{
    private readonly FunctionId _functionId;
    private readonly Dictionary<StateId, StoredState> _storedStates;
    private readonly IFunctionStore _functionStore;
    private readonly IStatesStore _statesStore;
    private readonly ISerializer _serializer;
    
    private string? _defaultStateJson;
    private FlowState? _defaultState;

    public ExistingStates(FunctionId functionId, string? defaultState, IEnumerable<StoredState> storedStates, IFunctionStore functionStore, ISerializer serializer)
    {
        _functionId = functionId;
        _functionStore = functionStore;
        _statesStore = functionStore.StatesStore;
        _serializer = serializer;

        _defaultStateJson = defaultState;
        _storedStates = storedStates.ToDictionary(s => s.StateId, s => s);
    }

    public IEnumerable<StateId> StateIds => _storedStates.Keys;
    public bool HasState(string stateId) => _storedStates.ContainsKey(stateId);
    public bool HasDefaultState() => _defaultStateJson != null;

    public TState Get<TState>() where TState : FlowState, new()
    {
        if (_defaultState != null)
            return (TState) _defaultState;
        if (_defaultStateJson != null)
            return (TState) (_defaultState = _serializer.DeserializeState<TState>(_defaultStateJson));

        var state = new TState();
        state.Initialize(() => _functionStore.SetDefaultState(_functionId, _serializer.SerializeState(state)));
        _defaultState = state;
        return state;
    }
    
    public TState Get<TState>(string stateId) where TState : FlowState, new()
    {
        if (!_storedStates.TryGetValue(stateId, out var storedState))
            throw new KeyNotFoundException($"State '{stateId}' was not found");

        var state = _serializer.DeserializeState<TState>(storedState.StateJson);
        state.Initialize(onSave: () => Set(stateId, state));
        return state;
    }

    public async Task RemoveDefault()
    {
        await _functionStore.SetDefaultState(_functionId, stateJson: null);
        _defaultState = null;
        _defaultStateJson = null;
    }
    public async Task Remove(string stateId)
    {
        await _statesStore.RemoveState(_functionId, stateId);
        _storedStates.Remove(stateId);
    }

    public async Task Set<TState>(TState state) where TState : FlowState, new()
    {
        _defaultState = state;
        await _functionStore.SetDefaultState(_functionId, _serializer.SerializeState(state));
    }
    
    public async Task Set<TState>(string stateId, TState state) where TState : FlowState, new()
    {
        var json = _serializer.SerializeState(state);
        var storedState = new StoredState(stateId, json);
        await _statesStore.UpsertState(_functionId, storedState);
        _storedStates[stateId] = storedState;
    }
}