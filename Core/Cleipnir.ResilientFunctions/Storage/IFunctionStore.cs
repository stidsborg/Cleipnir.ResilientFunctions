using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    public ITypeStore TypeStore { get; }
    public IMessageStore MessageStore { get; }
    public IEffectsStore EffectsStore { get; }
    public ITimeoutStore TimeoutStore { get; }
    public ICorrelationStore CorrelationStore { get; }
    public Utilities Utilities { get; }
    public IMigrator Migrator { get; }
    public Task Initialize();
    
    Task<bool> CreateFunction(
        FlowId flowId, 
        byte[]? param,
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
        byte[]? param,
        byte[]? result,
        int expectedEpoch
    );
    
    Task<bool> SetFunctionState(
        FlowId flowId,
        Status status,
        byte[]? param,
        byte[]? result,
        StoredException? storedException,
        long expires,
        int expectedEpoch
    );

    Task<bool> SucceedFunction(
        FlowId flowId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> PostponeFunction(
        FlowId flowId,
        long postponeUntil, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> FailFunction(
        FlowId flowId, 
        StoredException storedException,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> SuspendFunction(
        FlowId flowId, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );

    Task<bool> Interrupt(FlowId flowId, bool onlyIfExecuting);
    Task<bool?> Interrupted(FlowId flowId); 

    Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId);
    Task<StoredFlow?> GetFunction(FlowId flowId);
    Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType, Status status);
    Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType);
    Task<IReadOnlyList<FlowType>> GetTypes();

    Task<bool> DeleteFunction(FlowId flowId);
}