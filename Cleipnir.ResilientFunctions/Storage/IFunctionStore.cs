using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    // ** CREATION ** // 
    Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param,
        string? scrapbookType,
        Status initialStatus,
        int initialEpoch,
        int initialSignOfLife
    );

    // ** LEADERSHIP ** //
    Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch);
    Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife);
        
    Task<IEnumerable<StoredFunctionStatus>> GetFunctionsWithStatus(
        FunctionTypeId functionTypeId, 
        Status status,
        long? expiresBefore = null
    ); //todo consider change this to async enumerable?
        
    // ** CHANGES ** //
    Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string? scrapbookJson,
        StoredResult? result,
        StoredFailure? failed,
        long? postponedUntil,
        int expectedEpoch
    );

    // ** GETTER ** //
    Task<StoredFunction?> GetFunction(FunctionId functionId);
}