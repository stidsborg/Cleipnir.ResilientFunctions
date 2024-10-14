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
    public ITimeoutStore TimeoutStore { get; }
    public ICorrelationStore CorrelationStore { get; }
    public Utilities Utilities { get; }
    public IMigrator Migrator { get; }
    public Task Initialize();
    
    Task<bool> CreateFunction(
        FlowId flowId, 
        string? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    );

    Task BulkScheduleFunctions(
        IEnumerable<IdWithParam> functionsWithParam
    );
    
    Task<StoredFlow?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration);
    
    Task<bool> RenewLease(FlowId flowId, int expectedEpoch, long leaseExpiration);
    
    Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore);
    Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(FlowType flowType, long completedBefore);
    
    Task<bool> SetParameters(
        FlowId flowId,
        string? param,
        string? result,
        int expectedEpoch
    );
    
    Task<bool> SetFunctionState(
        FlowId flowId,
        Status status,
        string? param,
        string? result,
        StoredException? storedException,
        long expires,
        int expectedEpoch
    );

    Task<bool> SucceedFunction(
        FlowId flowId, 
        string? result, 
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> PostponeFunction(
        FlowId flowId,
        long postponeUntil, 
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> FailFunction(
        FlowId flowId, 
        StoredException storedException,
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> SuspendFunction(
        FlowId flowId, 
        long expectedInterruptCount,
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );

    Task SetDefaultState(FlowId flowId, string? stateJson);

    Task<bool> Interrupt(FlowId flowId);
    Task<bool> IncrementInterruptCount(FlowId flowId);
    Task<long?> GetInterruptCount(FlowId flowId); 

    Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId);
    Task<StoredFlow?> GetFunction(FlowId flowId);
    Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType, Status status);
    Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType);
    Task<IReadOnlyList<FlowType>> GetTypes();

    Task<bool> DeleteFunction(FlowId flowId);
}