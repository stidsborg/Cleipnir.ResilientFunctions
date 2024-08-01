using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class States
{
    private readonly FlowId _flowId;
    private readonly IStatesStore _statesStore;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private Dictionary<StateId, StoredState>? _existingStoredStates;
    private readonly Dictionary<StateId, FlowState> _existingStates = new();

    private readonly string? _defaultStateJson;
    private FlowState? _defaultState;
    private Func<string>? _defaultStateSerializer = null;
    
    private readonly object _sync = new();

    public States(
        FlowId flowId, 
        string? defaultStateJson,
        IFunctionStore functionStore, 
        IStatesStore statesStore, 
        ISerializer serializer)
    {
        _flowId = flowId;
        _statesStore = statesStore;
        _functionStore = functionStore;
        _serializer = serializer;
        _defaultStateJson = defaultStateJson;
    }

    private async Task<Dictionary<StateId, StoredState>> GetExistingStoredStates()
    {
        lock (_sync)
            if (_existingStoredStates is not null) 
                return _existingStoredStates;

        var existingStates = await _statesStore.GetStates(_flowId);
        var existingStatesDict = existingStates
            .ToDictionary(s => s.StateId, s => s);
        
        lock (_sync) 
            return _existingStoredStates ??= existingStatesDict;
    }

    public T CreateOrGet<T>() where T : FlowState, new()
    {
        lock (_sync)
            if (_defaultState != null)
                return (T) _defaultState;
            else if (_defaultStateJson != null)
                return (T)(_defaultState = _serializer.DeserializeState<T>(_defaultStateJson));
            else
            {
                var newState = new T();
                newState.Initialize(
                    onSave: () => _functionStore.SetDefaultState(_flowId, _serializer.SerializeState(newState))
                );
                _defaultStateSerializer = () => _serializer.SerializeState(newState);
                _defaultState = newState;
                return newState;
            }
    }

    internal string? SerializeDefaultState()
    {
        lock (_sync) 
            return _defaultStateSerializer == null 
                ? _defaultStateJson 
                : _defaultStateSerializer();
    }

    public async Task<T> CreateOrGet<T>(string id) where T : FlowState, new()
    {
        if (string.IsNullOrEmpty(id))
            throw new ArgumentException("Id must not be empty string or null", nameof(id));

        var existingStoredStates = await GetExistingStoredStates();
        
        lock (_sync)
            if (_existingStates.TryGetValue(id, out var state))
                return (T)state;
            else if (existingStoredStates.TryGetValue(key: id, out var storedState))
            {
                var s = _serializer.DeserializeState<T>(storedState.StateJson);
                _existingStates[id] = s;
                s.Initialize(onSave: () => SaveState(id, s));
                return s;
            }
            else
            {
                var newState = new T();
                newState.Initialize(onSave: () => SaveState(id, newState));
                _existingStates[id] = newState;
                existingStoredStates[id] = new StoredState(id, _serializer.SerializeState(newState));
                return newState;
            }
    }
    
    public async Task Remove(string id)
    {
        var existingStoredStates = await GetExistingStoredStates(); 
        
        if (string.IsNullOrEmpty(id))
            throw new ArgumentException("Id must not be empty string or null", nameof(id));
        
        lock (_sync)
            if (!existingStoredStates.ContainsKey(id))
                return;
        
        await _statesStore.RemoveState(_flowId, id);

        lock (_sync)
        {
            existingStoredStates.Remove(id);
            _existingStates.Remove(id);
        }
    }

    private async Task SaveState<T>(string id, T state) where T : FlowState, new()
    {
        var json = _serializer.SerializeState(state);
        var storedState = new StoredState(new StateId(id), json);
        await _statesStore.UpsertState(_flowId, storedState);
    }
}