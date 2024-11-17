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
    public ILogStore LogStore { get; }
    public Task Initialize();
    
    Task<bool> CreateFunction(
        StoredId storedId, 
        FlowInstance humanInstanceId,
        byte[]? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    );
    
    Task BulkScheduleFunctions(
        IEnumerable<IdWithParam> functionsWithParam
    );
    
    Task<StoredFlow?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration);
    
    Task<bool> RenewLease(StoredId storedId, int expectedEpoch, long leaseExpiration);
    
    Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore);
    Task<IReadOnlyList<StoredInstance>> GetSucceededFunctions(StoredType storedType, long completedBefore);
    
    Task<bool> SetParameters(
        StoredId storedId,
        byte[]? param,
        byte[]? result,
        int expectedEpoch
    );
    
    Task<bool> SetFunctionState(
        StoredId storedId,
        Status status,
        byte[]? param,
        byte[]? result,
        StoredException? storedException,
        long expires,
        int expectedEpoch
    );

    Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );
    
    Task<bool> SuspendFunction(
        StoredId storedId, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    );

    Task<bool> Interrupt(StoredId storedId, bool onlyIfExecuting);
    Task<bool?> Interrupted(StoredId storedId); 

    Task<StatusAndEpoch?> GetFunctionStatus(StoredId storedId);
    Task<StoredFlow?> GetFunction(StoredId storedId);
    Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType, Status status);
    Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType);

    Task<bool> DeleteFunction(StoredId storedId);
}