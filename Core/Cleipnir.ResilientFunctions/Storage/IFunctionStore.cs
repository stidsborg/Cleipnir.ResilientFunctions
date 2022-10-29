using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    public Task Initialize();
    
    Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param,
        StoredScrapbook storedScrapbook,
        long crashedCheckFrequency,
        int version
    );
    
    Task<bool> IncrementEpoch(FunctionId functionId, int expectedEpoch);
    Task<bool> RestartExecution(
        FunctionId functionId,
        Tuple<StoredParameter, StoredScrapbook>? paramAndScrapbook, 
        int expectedEpoch,
        long crashedCheckFrequency,
        int version
    );
    
    Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife);

    Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId, int versionUpperBound);
    Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore, int versionUpperBound);
    
    Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        StoredParameter storedParameter,
        StoredScrapbook storedScrapbook,
        StoredResult storedResult,
        StoredException? storedException,
        long? postponeUntil,
        int expectedEpoch
    );

    Task<bool> SaveScrapbookForExecutingFunction(FunctionId functionId, string scrapbookJson, int expectedEpoch);
    Task<bool> SetParameters(
        FunctionId functionId,
        StoredParameter? storedParameter,
        StoredScrapbook? storedScrapbook,
        int expectedEpoch
    );

    Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch);
    Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch);
    Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch);

    Task<StoredFunction?> GetFunction(FunctionId functionId);
    Task<StoredFunctionStatus?> GetFunctionStatus(FunctionId functionId);

    Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null, Status? expectedStatus = null);
}