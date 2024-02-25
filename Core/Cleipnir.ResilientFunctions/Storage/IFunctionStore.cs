using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    public IMessageStore MessageStore { get; }
    public IActivityStore ActivityStore { get; }
    public ITimeoutStore TimeoutStore { get; }
    public Utilities Utilities { get; }
    public Task Initialize();
    
    Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param,
        StoredState storedState,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    );
    
    Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration);
    
    Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration);

    Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore);
    Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore);

    Task<bool> SaveStateForExecutingFunction(
        FunctionId functionId,
        string stateJson,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    );
    
    Task<bool> SetParameters(
        FunctionId functionId,
        StoredParameter storedParameter,
        StoredState storedState,
        StoredResult storedResult,
        int expectedEpoch
    );
    
    Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        StoredParameter storedParameter,
        StoredState storedState,
        StoredResult storedResult,
        StoredException? storedException,
        long? postponeUntil,
        int expectedEpoch
    );

    Task<bool> SucceedFunction(
        FunctionId functionId, 
        StoredResult result, 
        string stateJson, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> PostponeFunction(
        FunctionId functionId, 
        long postponeUntil, 
        string stateJson,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException, 
        string stateJson, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> SuspendFunction(
        FunctionId functionId, 
        int expectedMessageCount, 
        string stateJson, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );

    Task IncrementSignalCount(FunctionId functionId);

    Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId);
    Task<StoredFunction?> GetFunction(FunctionId functionId);

    Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null);
}