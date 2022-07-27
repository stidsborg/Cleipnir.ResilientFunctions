using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    public Task Initialize();
    
    // ** CREATION ** // 
    Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param,
        string? scrapbookType,
        long crashedCheckFrequency,
        int version
    );

    // ** LEADERSHIP ** //
    Task<bool> TryToBecomeLeader(
        FunctionId functionId, 
        Status newStatus, 
        int expectedEpoch, 
        int newEpoch, 
        long crashedCheckFrequency,
        int version,
        Option<string> scrapbookJson
    );
    
    Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife);

    Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId, int versionUpperBound);
    Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore, int versionUpperBound);

    // ** CHANGES ** //
    Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string? scrapbookJson,
        StoredResult? result,
        string? errorJson,
        long? postponedUntil,
        int expectedEpoch
    );

    // ** GETTER ** //
    Task<StoredFunction?> GetFunction(FunctionId functionId);
}