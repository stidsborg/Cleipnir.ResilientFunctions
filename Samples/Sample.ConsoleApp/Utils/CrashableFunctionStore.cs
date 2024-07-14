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
    public IStatesStore StatesStore => _inner.StatesStore;
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public ICorrelationStore CorrelationStore => _inner.CorrelationStore;
    public Utilities Utilities => _inner.Utilities;
    
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

    public Task BulkScheduleFunctions(IEnumerable<FunctionIdWithParam> functionsWithParam)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.BulkScheduleFunctions(functionsWithParam);

    public Task<StoredFunction?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration)
        => _crashed
            ? Task.FromException<StoredFunction?>(new TimeoutException())
            : _inner.RestartExecution(flowId, expectedEpoch, leaseExpiration);
    
    public Task<bool> RenewLease(FlowId flowId, int expectedEpoch, long leaseExpiration)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.RenewLease(flowId, expectedEpoch, leaseExpiration);

    public Task<IReadOnlyList<InstanceIdAndEpoch>> GetCrashedFunctions(FlowType flowType, long leaseExpiresBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<InstanceIdAndEpoch>>(new TimeoutException())
            : _inner.GetCrashedFunctions(flowType, leaseExpiresBefore);

    public Task<IReadOnlyList<InstanceIdAndEpoch>> GetPostponedFunctions(FlowType flowType, long isEligibleBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<InstanceIdAndEpoch>>(new TimeoutException())
            : _inner.GetPostponedFunctions(flowType, isEligibleBefore);

    public Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(FlowType flowType, long completedBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<FlowInstance>>(new TimeoutException())
            : _inner.GetSucceededFunctions(flowType, completedBefore);

    public Task<bool> SetFunctionState(
        FlowId flowId, Status status, string? storedParameter,
        string? storedResult, StoredException? storedException, 
        long? postponeUntil, 
        int expectedEpoch
    ) => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetFunctionState(
                flowId, status, 
                storedParameter, storedResult, 
                storedException, postponeUntil, 
                expectedEpoch
            );

    public Task<bool> SucceedFunction(
        FlowId flowId,
        string? result,
        string? defaultState,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.SucceedFunction(flowId, result, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> PostponeFunction(
        FlowId flowId, 
        long postponeUntil, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.PostponeFunction(flowId, postponeUntil, defaultState, timestamp, expectedEpoch, complimentaryState); 

    public Task<bool> FailFunction(
        FlowId flowId, 
        StoredException storedException, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.FailFunction(flowId, storedException, defaultState, timestamp, expectedEpoch, complimentaryState); 

    public Task<bool> SuspendFunction(
        FlowId flowId, 
        long expectedInterruptCount, 
        string? defaultState, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.SuspendFunction(flowId, expectedInterruptCount, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task SetDefaultState(FlowId flowId, string? stateJson)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.SetDefaultState(flowId, stateJson);

    public Task<bool> SetParameters(FlowId flowId, string? storedParameter, string? storedResult, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetParameters(flowId, storedParameter, storedResult, expectedEpoch);
    
    public Task<bool> IncrementInterruptCount(FlowId flowId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.IncrementInterruptCount(flowId);

    public Task<long?> GetInterruptCount(FlowId flowId)
        => _crashed
            ? Task.FromException<long?>(new TimeoutException())
            : _inner.GetInterruptCount(flowId);

    public Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId)
        => _crashed
            ? Task.FromException<StatusAndEpoch?>(new TimeoutException())
            : _inner.GetFunctionStatus(flowId);

    public Task<StoredFunction?> GetFunction(FlowId flowId)
        => _crashed
            ? Task.FromException<StoredFunction?>(new TimeoutException())
            : _inner.GetFunction(flowId);

    public Task<bool> DeleteFunction(FlowId flowId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.DeleteFunction(flowId);
}