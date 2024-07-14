using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryStatesStore : IStatesStore
{
    private readonly Dictionary<FlowId, Dictionary<StateId, StoredState>> _states = new();
    private readonly object _sync = new();
    
    public Task Initialize() => Task.CompletedTask;
    
    public Task Truncate()
    {
        lock (_sync)
            _states.Clear();

        return Task.CompletedTask;
    }

    public Task UpsertState(FlowId flowId, StoredState storedState)
    {
        lock (_sync)
        {
            AddDictionaryIfNotExists(flowId);
            _states[flowId][storedState.StateId] = storedState;
        }
        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredState>> GetStates(FlowId flowId)
    {
        lock (_sync)
        {
            AddDictionaryIfNotExists(flowId);
            return _states[flowId].Values.ToList().AsEnumerable().ToTask();
        }
            
    }

    public Task RemoveState(FlowId flowId, StateId stateId)
    {
        lock (_sync)
        {
            AddDictionaryIfNotExists(flowId);
            _states[flowId].Remove(stateId);
        }

        return Task.CompletedTask;
    }

    public Task Remove(FlowId flowId)
    {
        lock (_sync)
            _states.Remove(flowId);

        return Task.CompletedTask;
    }

    private void AddDictionaryIfNotExists(FlowId flowId)
    {
        lock (_sync)
            if (!_states.ContainsKey(flowId))
                _states[flowId] = new Dictionary<StateId, StoredState>();
    }
}