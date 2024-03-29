using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class States
{
    private readonly FunctionId _functionId;
    private readonly IStatesStore _statesStore;
    private readonly ISerializer _serializer;
    private readonly Dictionary<StateId, WorkflowState> _existingStates;

    private readonly object _sync = new();

    public States(FunctionId functionId, Dictionary<StateId, WorkflowState> existingStates, IStatesStore statesStore, ISerializer serializer)
    {
        _functionId = functionId;
        _statesStore = statesStore;
        _serializer = serializer;

        _existingStates = existingStates;
    }

    public T GetOrCreate<T>() where T : WorkflowState, new()
        => InnerGetOrCreate<T>(id: "");

    public T GetOrCreate<T>(string id) where T : WorkflowState, new()
    {
        if (string.IsNullOrEmpty(id))
            throw new ArgumentException("Id must not be empty string or null", nameof(id));

        return InnerGetOrCreate<T>(id);
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

    private T InnerGetOrCreate<T>(string id) where T : WorkflowState, new()
    {
        lock (_sync)
            if (_existingStates.TryGetValue(key: id, out var state))
                return (T)state;
            else
            {
                var newState = new T();
                newState.Initialize(onSave: () => SaveState(id, newState));
                _existingStates[id] = newState;
                return newState;
            }
    }

    private async Task SaveState<T>(string id, T state) where T : WorkflowState, new()
    {
        var (json, type) = _serializer.SerializeState(state);
        var storedState = new StoredState(new StateId(id), json, type);
        await _statesStore.UpsertState(_functionId, storedState);
    }
}