using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.LeaseUpdaterTests;

public class LeaseUpdaterTestFunctionStore : IFunctionStore
{
    public delegate bool LeaseUpdaterCallback(FunctionId functionId, int expectedEpoch, long newLeaseExpiry);

    private readonly LeaseUpdaterCallback _leaseUpdaterCallback;
    private readonly IFunctionStore _inner = new InMemoryFunctionStore();

    public LeaseUpdaterTestFunctionStore(LeaseUpdaterCallback leaseUpdaterCallback) => _leaseUpdaterCallback = leaseUpdaterCallback;

    public IMessageStore MessageStore => _inner.MessageStore;
    public IActivityStore ActivityStore => _inner.ActivityStore;
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public Utilities Utilities => _inner.Utilities;
    public Task Initialize() => _inner.Initialize();

    public Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        StoredScrapbook storedScrapbook, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    ) => _inner.CreateFunction(functionId, param, storedScrapbook, leaseExpiration, postponeUntil, timestamp);
    
    public Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
        => _inner.RestartExecution(functionId, expectedEpoch, leaseExpiration);
    
    public Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        var success = _leaseUpdaterCallback(functionId, expectedEpoch, leaseExpiration);
        return success.ToTask();
    }

    public Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
        => _inner.GetCrashedFunctions(functionTypeId, leaseExpiresBefore);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
        => _inner.GetPostponedFunctions(functionTypeId, isEligibleBefore);

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status, 
        StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil, 
        int expectedEpoch
    ) => _inner.SetFunctionState(functionId, status, storedParameter, storedScrapbook, storedResult, storedException, postponeUntil, expectedEpoch);

    public Task<bool> SaveScrapbookForExecutingFunction( 
        FunctionId functionId,
        string scrapbookJson,
        int expectedEpoch,
        ComplimentaryState.SaveScrapbookForExecutingFunction complimentaryState) 
    => _inner.SaveScrapbookForExecutingFunction(functionId, scrapbookJson, expectedEpoch, complimentaryState);

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult, int expectedEpoch)
        => _inner.SetParameters(functionId, storedParameter, storedScrapbook, storedResult, expectedEpoch);

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, long timestamp, int expectedEpoch, ComplimentaryState.SetResult complementaryState)
        => _inner.SucceedFunction(functionId, result, scrapbookJson, timestamp, expectedEpoch, complementaryState);

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, long timestamp, int expectedEpoch, ComplimentaryState.SetResult complementaryState)
        => _inner.PostponeFunction(functionId, postponeUntil, scrapbookJson, timestamp, expectedEpoch, complementaryState);

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, long timestamp, int expectedEpoch, ComplimentaryState.SetResult complementaryState)
        => _inner.FailFunction(functionId, storedException, scrapbookJson, timestamp, expectedEpoch, complementaryState);

    public Task<bool> SuspendFunction(FunctionId functionId, int expectedMessageCount, string scrapbookJson, long timestamp, int expectedEpoch, ComplimentaryState.SetResult complementaryState)
        => _inner.SuspendFunction(functionId, expectedMessageCount, scrapbookJson, timestamp, expectedEpoch, complementaryState);

    public Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
        => _inner.GetFunctionStatus(functionId);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _inner.GetFunction(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
        => _inner.DeleteFunction(functionId, expectedEpoch);
}