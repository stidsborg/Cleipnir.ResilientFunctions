using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    public IEventStore EventStore { get; }
    public ITimeoutStore TimeoutStore { get; }
    public Utilities Utilities { get; }
    public Task Initialize();
    
    Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param,
        StoredScrapbook storedScrapbook,
        IEnumerable<StoredEvent>? storedEvents,
        long leaseExpiration,
        long? postponeUntil
    );
    
    Task<bool> IncrementAlreadyPostponedFunctionEpoch(FunctionId functionId, int expectedEpoch);
    Task<bool> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration);
    
    Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration);

    Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore);
    Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore);

    Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        StoredParameter storedParameter,
        StoredScrapbook storedScrapbook,
        StoredResult storedResult,
        StoredException? storedException,
        long? postponeUntil,
        ReplaceEvents? events,
        int expectedEpoch
    );

    Task<bool> SaveScrapbookForExecutingFunction(
        FunctionId functionId,
        string scrapbookJson,
        int expectedEpoch,
        ComplimentaryState.SaveScrapbookForExecutingFunction complimentaryState
    );
    
    Task<bool> SetParameters(
        FunctionId functionId,
        StoredParameter storedParameter,
        StoredScrapbook storedScrapbook,
        ReplaceEvents? events,
        bool suspended,
        int expectedEpoch
    );

    Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult complementaryState);
    Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult complementaryState);
    Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult complementaryState);
    Task<SuspensionResult> SuspendFunction(FunctionId functionId, int expectedEventCount, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult complementaryState);

    Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId);
    Task<StoredFunction?> GetFunction(FunctionId functionId);

    Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null);
}