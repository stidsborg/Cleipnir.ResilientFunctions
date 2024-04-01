using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryStatesStore : IStatesStore
{
    private readonly Dictionary<FunctionId, Dictionary<StateId, StoredState>> _states = new();
    private readonly object _sync = new();
    
    public Task Initialize() => Task.CompletedTask;
    
    public Task Truncate()
    {
        lock (_sync)
            _states.Clear();

        return Task.CompletedTask;
    }

    public Task UpsertState(FunctionId functionId, StoredState storedState)
    {
        lock (_sync)
        {
            AddDictionaryIfNotExists(functionId);
            _states[functionId][storedState.StateId] = storedState;
        }
        return Task.CompletedTask;
    }

    public Task<IEnumerable<StoredState>> GetStates(FunctionId functionId)
    {
        lock (_sync)
        {
            AddDictionaryIfNotExists(functionId);
            return _states[functionId].Values.ToList().AsEnumerable().ToTask();
        }
            
    }

    public Task RemoveState(FunctionId functionId, StateId stateId)
    {
        lock (_sync)
        {
            AddDictionaryIfNotExists(functionId);
            _states[functionId].Remove(stateId);
        }

        return Task.CompletedTask;
    }

    public Task Remove(FunctionId functionId)
    {
        lock (_sync)
            _states.Remove(functionId);

        return Task.CompletedTask;
    }

    private void AddDictionaryIfNotExists(FunctionId functionId)
    {
        lock (_sync)
            if (!_states.ContainsKey(functionId))
                _states[functionId] = new Dictionary<StateId, StoredState>();
    }
}