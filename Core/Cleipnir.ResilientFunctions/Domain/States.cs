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
    private readonly ISerializer _serializer;
    private readonly Dictionary<StateId, StoredState> _existingStoredStates;
    private readonly Dictionary<StateId, WorkflowState> _existingStates;

    private readonly object _sync = new();

    public States(FunctionId functionId, IEnumerable<StoredState> existingStates, IStatesStore statesStore, ISerializer serializer)
    {
        _functionId = functionId;
        _statesStore = statesStore;
        _serializer = serializer;

        _existingStoredStates = existingStates.ToDictionary(s => s.StateId, s => s);
        _existingStates = new();
    }

    public T CreateOrGet<T>() where T : WorkflowState, new()
        => InnerGetOrCreate<T>(id: "");

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

        await InnerRemove(id);
    }
    
    public Task RemoveDefault() => InnerRemove(id: "");
    
    private async Task InnerRemove(string id)
    {
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