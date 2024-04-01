using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IStatesStore
{
    Task Initialize();
    Task UpsertState(FunctionId functionId, StoredState storedState);
    Task<IEnumerable<StoredState>> GetStates(FunctionId functionId);
    Task RemoveState(FunctionId functionId, StateId stateId);
    Task Remove(FunctionId functionId);
}