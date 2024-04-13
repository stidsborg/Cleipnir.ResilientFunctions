using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class StateFetcher
{
    private readonly IStatesStore _statesStore;
    private readonly ISerializer _serializer;

    public StateFetcher(IStatesStore statesStore, ISerializer serializer)
    {
        _statesStore = statesStore;
        _serializer = serializer;
    }

    public async Task<TState?> FetchState<TState>(FunctionId functionId, StateId? stateId) where TState : WorkflowState, new() 
    {
        stateId ??= new StateId("");
        var storedStates = await _statesStore.GetStates(functionId);
        
        foreach (var storedState in storedStates)
            if (storedState.StateId == stateId)
            {
                var state = _serializer.DeserializeState<TState>(storedState.StateJson);
                state.Initialize(
                    onSave: () => _statesStore.UpsertState(functionId, new StoredState(stateId, StateJson: _serializer.SerializeState(state)))
                );

                return state;
            }
        
        return null;
    }
}