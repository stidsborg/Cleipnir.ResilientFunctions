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
    public ISemaphoreStore SemaphoreStore { get; }
    public Task Initialize();
    
    Task<bool> CreateFunction(
        StoredId storedId, 
        FlowInstance humanInstanceId,
        byte[]? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent
    );
    
    Task BulkScheduleFunctions(
        IEnumerable<IdWithParam> functionsWithParam,
        StoredId? parent
    );
    
    Task<StoredFlow?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration);
    
    Task<int> RenewLeases(IReadOnlyList<LeaseUpdate> leaseUpdates, long leaseExpiration);
    
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
    Task Interrupt(IEnumerable<StoredId> storedIds);
    Task<bool?> Interrupted(StoredId storedId); 

    Task<StatusAndEpoch?> GetFunctionStatus(StoredId storedId);
    Task<IReadOnlyList<StatusAndEpochWithId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds);
    Task<StoredFlow?> GetFunction(StoredId storedId);
    Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType, Status status);
    Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType);

    Task<bool> DeleteFunction(StoredId storedId);
}