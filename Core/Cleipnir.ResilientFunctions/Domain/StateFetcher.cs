using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class StateFetcher
{
    private readonly IFunctionStore _functionStore;
    private readonly IStatesStore _statesStore;
    private readonly ISerializer _serializer;

    public StateFetcher(IFunctionStore functionStore, ISerializer serializer)
    {
        _functionStore = functionStore;
        _statesStore = functionStore.StatesStore;
        _serializer = serializer;
    }

    public async Task<TState?> FetchState<TState>(FunctionId functionId) where TState : WorkflowState, new() 
    {
        var sf = await _functionStore.GetFunction(functionId);
        if (sf == null || sf.DefaultState == null)
            return null;
        
        var state = _serializer.DeserializeState<TState>(sf.DefaultState);
        state.Initialize(
            onSave: () => throw new InvalidOperationException("State cannot be modified from the outside - except when using control-panel")
        );

        return state;
    }
    
    public async Task<TState?> FetchState<TState>(FunctionId functionId, StateId stateId) where TState : WorkflowState, new() 
    {
        stateId ??= new StateId("");
        var storedStates = await _statesStore.GetStates(functionId);
        
        foreach (var storedState in storedStates)
            if (storedState.StateId == stateId)
            {
                var state = _serializer.DeserializeState<TState>(storedState.StateJson);
                state.Initialize(
                    onSave: () => throw new InvalidOperationException("State cannot be modified from the outside - except when using control-panel")
                );

                return state;
            }
        
        return null;
    }
}