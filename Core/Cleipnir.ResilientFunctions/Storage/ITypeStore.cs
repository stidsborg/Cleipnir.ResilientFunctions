using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ITypeStore
{
    public Task<int> InsertOrGetFlowType(FlowType flowType);
    public Task<IReadOnlyDictionary<FlowType, int>> GetAllFlowTypes();
}