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
    private readonly IStatesStore _statesStore;
    private readonly ISerializer _serializer;

    public ExistingStates(FunctionId functionId, IEnumerable<StoredState> storedStates, IStatesStore statesStore, ISerializer serializer)
    {
        _functionId = functionId;
        _storedStates = storedStates.ToDictionary(s => s.StateId, s => s);
        _statesStore = statesStore;
        _serializer = serializer;
    }

    public IEnumerable<StateId> StateIds => _storedStates.Keys;
    public bool HasState(string stateId) => _storedStates.ContainsKey(stateId);
    public bool HasDefaultState() => _storedStates.ContainsKey("");

    public TState Get<TState>() where TState : WorkflowState, new()
        => Get<TState>(stateId: "");
    public TState Get<TState>(string stateId) where TState : WorkflowState, new()
    {
        if (!_storedStates.TryGetValue(stateId, out var storedState))
            throw new KeyNotFoundException($"State '{stateId}' was not found");

        var state = _serializer.DeserializeState<TState>(storedState.StateJson);
        state.Initialize(onSave: () => Set(stateId, state));
        return state;
    }

    public Task RemoveDefault() => Remove(stateId: "");
    public async Task Remove(string stateId)
    {
        await _statesStore.RemoveState(_functionId, stateId);
        _storedStates.Remove(stateId);
    }

    public Task Set<TState>(TState state) where TState : WorkflowState, new()
        => Set(stateId: "", state);
    public async Task Set<TState>(string stateId, TState state) where TState : WorkflowState, new()
    {
        var json = _serializer.SerializeState(state);
        var storedState = new StoredState(stateId, json);
        await _statesStore.UpsertState(_functionId, storedState);
        _storedStates[stateId] = storedState;
    }
}