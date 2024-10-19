using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class StateFetcher(IEffectsStore effectsStore, ISerializer serializer)
{
    public Task<TState?> FetchState<TState>(FlowId flowId) where TState : FlowState, new()
        => FetchState<TState>(flowId, stateId: "");
    
    public async Task<TState?> FetchState<TState>(FlowId flowId, StateId stateId) where TState : FlowState, new() 
    {
        stateId ??= new StateId("");
        var storedStates = await effectsStore.GetEffectResults(flowId);
        
        foreach (var storedEffect in storedStates)
            if (storedEffect.EffectId == stateId.Value)
            {
                var state = serializer.DeserializeState<TState>(storedEffect.Result!);
                state.Initialize(
                    onSave: () => throw new InvalidOperationException("State cannot be modified outside of an executing flow - except when using control-panel")
                );

                return state;
            }
        
        return null;
    }
}