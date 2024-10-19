using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.Utils;

public class CrashableFunctionStore : IFunctionStore
{
    private readonly IFunctionStore _inner;
    private volatile bool _crashed;

    private readonly object _sync = new();
    public IMessageStore MessageStore => _inner.MessageStore;
    public IEffectsStore EffectsStore => _inner.EffectsStore;
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public ICorrelationStore CorrelationStore => _inner.CorrelationStore;
    public Utilities Utilities => _inner.Utilities;
    public IMigrator Migrator => _inner.Migrator;

    public CrashableFunctionStore(IFunctionStore inner) => _inner = inner;

    public void Crash() => _crashed = true;

    public Task Initialize() => Task.CompletedTask;

    public Task<bool> CreateFunction(
        FlowId flowId,
        string? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.CreateFunction(
            flowId,
            param,
            leaseExpiration,
            postponeUntil,
            timestamp
        );

    public Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.BulkScheduleFunctions(functionsWithParam);

    public Task<StoredFlow?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration)
        => _crashed
            ? Task.FromException<StoredFlow?>(new TimeoutException())
            : _inner.RestartExecution(flowId, expectedEpoch, leaseExpiration);
    
    public Task<bool> RenewLease(FlowId flowId, int expectedEpoch, long leaseExpiration)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.RenewLease(flowId, expectedEpoch, leaseExpiration);

    public Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<IdAndEpoch>>(new TimeoutException())
            : _inner.GetExpiredFunctions(expiresBefore);

    public Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(FlowType flowType, long completedBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<FlowInstance>>(new TimeoutException())
            : _inner.GetSucceededFunctions(flowType, completedBefore);

    public Task<bool> SetFunctionState(
        FlowId flowId, Status status, string? storedParameter,
        string? storedResult, StoredException? storedException, 
        long expires, 
        int expectedEpoch
    ) => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetFunctionState(
                flowId, status, 
                storedParameter, storedResult, 
                storedException, expires, 
                expectedEpoch
            );

    public Task<bool> SucceedFunction(
        FlowId flowId,
        string? result,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.SucceedFunction(flowId, result, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> PostponeFunction(
        FlowId flowId, 
        long postponeUntil, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.PostponeFunction(flowId, postponeUntil, timestamp, expectedEpoch, complimentaryState); 

    public Task<bool> FailFunction(
        FlowId flowId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.FailFunction(flowId, storedException, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> SuspendFunction(FlowId flowId, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _crashed ? Task.FromException<bool>(new TimeoutException())
            : _inner.SuspendFunction(flowId, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> Interrupt(FlowId flowId, bool onlyIfExecuting)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.Interrupt(flowId, onlyIfExecuting);

    public Task<bool?> Interrupted(FlowId flowId)
        => _crashed
            ? Task.FromException<bool?>(new TimeoutException())
            : _inner.Interrupted(flowId);
    
    public Task<bool> SetParameters(FlowId flowId, string? storedParameter, string? storedResult, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetParameters(flowId, storedParameter, storedResult, expectedEpoch);
    
    public Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId)
        => _crashed
            ? Task.FromException<StatusAndEpoch?>(new TimeoutException())
            : _inner.GetFunctionStatus(flowId);

    public Task<StoredFlow?> GetFunction(FlowId flowId)
        => _crashed
            ? Task.FromException<StoredFlow?>(new TimeoutException())
            : _inner.GetFunction(flowId);

    public Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType, Status status)
        => _crashed
            ? Task.FromException<IReadOnlyList<FlowInstance>>(new TimeoutException())
            : _inner.GetInstances(flowType, status);

    public Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType)
        => _crashed
            ? Task.FromException<IReadOnlyList<FlowInstance>>(new TimeoutException())
            : _inner.GetInstances(flowType);

    public Task<IReadOnlyList<FlowType>> GetTypes()
        => _crashed
            ? Task.FromException<IReadOnlyList<FlowType>>(new TimeoutException())
            : _inner.GetTypes();

    public Task<bool> DeleteFunction(FlowId flowId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.DeleteFunction(flowId);
}