using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IStatesStore
{
    Task Initialize();
    Task Truncate();
    Task UpsertState(FlowId flowId, StoredState storedState);
    Task<IEnumerable<StoredState>> GetStates(FlowId flowId);
    Task RemoveState(FlowId flowId, StateId stateId);
    Task Remove(FlowId flowId);
}