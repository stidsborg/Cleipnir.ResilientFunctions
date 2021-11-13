using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage
{
    public interface IFunctionStore
    {
        Task<bool> StoreFunction(
            FunctionId functionId, 
            Parameter param1,
            Parameter? param2,
            string? scrapbookType, 
            long initialSignOfLife
        );
        
        Task<bool> UpdateScrapbook(
            FunctionId functionId, 
            string scrapbookJson,
            int expectedVersionStamp, 
            int newVersionStamp
        );
        
        Task<IEnumerable<NonCompletedFunction>> GetNonCompletedFunctions(FunctionTypeId functionTypeId);
        
        Task<bool> UpdateSignOfLife(FunctionId functionId, long expectedSignOfLife, long newSignOfLife);
        
        Task StoreFunctionResult(FunctionId functionId, string resultJson, string resultType);
        Task<Result?> GetFunctionResult(FunctionId functionId);
        Task<StoredFunction?> GetFunction(FunctionId functionId);
    }

    public record NonCompletedFunction(FunctionInstanceId InstanceId, long LastSignOfLife);
}