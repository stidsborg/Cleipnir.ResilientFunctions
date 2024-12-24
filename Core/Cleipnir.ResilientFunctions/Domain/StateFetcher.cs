using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class StateFetcher(StoredType storedType, IEffectsStore effectsStore, ISerializer serializer)
{
    public Task<TState?> FetchState<TState>(FlowInstance instanceId) where TState : FlowState, new()
        => FetchState<TState>(instanceId, stateId: "");
    
    public async Task<TState?> FetchState<TState>(FlowInstance instanceId, StateId stateId) where TState : FlowState, new()
    {
        var storedId = new StoredId(storedType, instanceId.ToStoredInstance());
        stateId ??= new StateId("");
        var storedStates = await effectsStore.GetEffectResults(storedId);
        
        foreach (var storedEffect in storedStates)
            if (storedEffect.EffectId.Id == stateId.Value)
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