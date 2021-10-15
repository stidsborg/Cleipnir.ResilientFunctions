using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage
{
    public interface IFunctionStore
    {
        Task<bool> StoreFunction(FunctionId functionId, string paramJson, string paramType, long initialSignOfLife);
        Task<IEnumerable<StoredFunction>> GetNonCompletedFunctions(FunctionTypeId functionTypeId, long olderThan);
        Task<bool> UpdateSignOfLife(FunctionId functionId, long expectedSignOfLife, long newSignOfLife);
        
        Task StoreFunctionResult(FunctionId functionId, string resultJson, string resultType);
        Task<FunctionResult?> GetFunctionResult(FunctionId functionId);
    }
}