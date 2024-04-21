using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    public IMessageStore MessageStore { get; }
    public IEffectsStore EffectsStore { get; }
    public IStatesStore StatesStore { get; }
    public ITimeoutStore TimeoutStore { get; }
    public Utilities Utilities { get; }
    public Task Initialize();
    
    Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    );
    
    Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration);
    
    Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration);

    Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore);
    Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore);
    
    Task<bool> SetParameters(
        FunctionId functionId,
        StoredParameter storedParameter,
        StoredResult storedResult,
        int expectedEpoch
    );
    
    Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        StoredParameter storedParameter,
        StoredResult storedResult,
        StoredException? storedException,
        long? postponeUntil,
        int expectedEpoch
    );

    Task<bool> SucceedFunction(
        FunctionId functionId, 
        StoredResult result, 
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> PostponeFunction(
        FunctionId functionId,
        long postponeUntil, 
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException,
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> SuspendFunction(
        FunctionId functionId, 
        long expectedInterruptCount,
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );

    Task SetDefaultState(FunctionId functionId, string? stateJson);

    Task<bool> IncrementInterruptCount(FunctionId functionId);
    Task<long?> GetInterruptCount(FunctionId functionId); 

    Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId);
    Task<StoredFunction?> GetFunction(FunctionId functionId);

    Task DeleteFunction(FunctionId functionId);
}