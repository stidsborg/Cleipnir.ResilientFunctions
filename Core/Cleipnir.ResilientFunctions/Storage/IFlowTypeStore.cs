using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFlowTypeStore
{
    public Task<StoredType> InsertOrGetStoredType(FlowType flowType);
    public Task<IReadOnlyDictionary<FlowType, StoredType>> GetAllFlowTypes();
}