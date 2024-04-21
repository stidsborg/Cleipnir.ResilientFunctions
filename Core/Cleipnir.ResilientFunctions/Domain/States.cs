using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class States
{
    private readonly FunctionId _functionId;
    private readonly IStatesStore _statesStore;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly Dictionary<StateId, StoredState> _existingStoredStates;
    private readonly Dictionary<StateId, WorkflowState> _existingStates;

    private string? _defaultStateJson;
    private WorkflowState? _defaultState;
    private Func<string>? _defaultStateSerializer = null;
    
    private readonly object _sync = new();

    public States(
        FunctionId functionId, 
        string? defaultStateJson,
        IEnumerable<StoredState> existingStates, 
        IFunctionStore functionStore, 
        IStatesStore statesStore, 
        ISerializer serializer)
    {
        _functionId = functionId;
        _statesStore = statesStore;
        _functionStore = functionStore;
        _serializer = serializer;
        _defaultStateJson = defaultStateJson;

        _existingStoredStates = existingStates.ToDictionary(s => s.StateId, s => s);
        _existingStates = new();
    }

    public T CreateOrGet<T>() where T : WorkflowState, new()
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
                    onSave: () => _functionStore.SetDefaultState(_functionId, _serializer.SerializeState(newState))
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

    public T CreateOrGet<T>(string id) where T : WorkflowState, new()
    {
        if (string.IsNullOrEmpty(id))
            throw new ArgumentException("Id must not be empty string or null", nameof(id));

        return InnerGetOrCreate<T>(id);
    }

    private T InnerGetOrCreate<T>(string id) where T : WorkflowState, new()
    {
        lock (_sync)
            if (_existingStates.TryGetValue(id, out var state))
                return (T)state;
            else if (_existingStoredStates.TryGetValue(key: id, out var storedState))
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
                return newState;
            }
    }
    
    public async Task Remove(string id)
    {
        if (string.IsNullOrEmpty(id))
            throw new ArgumentException("Id must not be empty string or null", nameof(id));
        
        lock (_sync)
        {
            var success = _existingStates.Remove(id);
            if (!success)
                return;
        }

        await _statesStore.RemoveState(_functionId, id);
    }

    private async Task SaveState<T>(string id, T state) where T : WorkflowState, new()
    {
        var json = _serializer.SerializeState(state);
        var storedState = new StoredState(new StateId(id), json);
        await _statesStore.UpsertState(_functionId, storedState);
    }
}