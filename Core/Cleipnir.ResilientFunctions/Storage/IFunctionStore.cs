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
    public ICorrelationStore CorrelationStore { get; }
    public Utilities Utilities { get; }
    public Task Initialize();
    
    Task<bool> CreateFunction(
        FunctionId functionId, 
        string? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    );

    Task BulkScheduleFunctions(
        IEnumerable<FunctionIdWithParam> functionsWithParam
    );
    
    Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration);
    
    Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration);

    Task<IReadOnlyList<InstanceIdAndEpoch>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore);
    Task<IReadOnlyList<InstanceIdAndEpoch>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore);
    Task<IReadOnlyList<FunctionInstanceId>> GetSucceededFunctions(FunctionTypeId functionTypeId, long completedBefore);
    
    Task<bool> SetParameters(
        FunctionId functionId,
        string? param,
        string? result,
        int expectedEpoch
    );
    
    Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string? param,
        string? result,
        StoredException? storedException,
        long? postponeUntil,
        int expectedEpoch
    );

    Task<bool> SucceedFunction(
        FunctionId functionId, 
        string? result, 
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

    Task<bool> DeleteFunction(FunctionId functionId);
}