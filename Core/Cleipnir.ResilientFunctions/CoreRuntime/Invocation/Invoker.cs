using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers.Disposables;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Invoker<TParam, TReturn> 
{
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly ReplicaId _replicaId;
    private readonly Func<TParam, Workflow, Task<Result<TReturn>>> _inner;
    
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly Utilities _utilities;
    private readonly FlowsTimeoutManager _flowsTimeoutManager;
    private readonly FlowsManager _flowsManager;

    internal Invoker(
        FlowType flowType, StoredType storedType,
        Func<TParam, Workflow, Task<Result<TReturn>>> inner,
        InvocationHelper<TParam, TReturn> invocationHelper,
        UnhandledExceptionHandler unhandledExceptionHandler,
        Utilities utilities,
        ReplicaId replicaId,
        FlowsTimeoutManager flowsTimeoutManager,
        FlowsManager flowsManager
    )
    {
        _flowType = flowType;
        _storedType = storedType;
        _replicaId = replicaId;
        _inner = inner;
        _invocationHelper = invocationHelper;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _utilities = utilities;
        _flowsTimeoutManager = flowsTimeoutManager;
        _flowsManager = flowsManager;
    }

    public async Task<InnerScheduled<TReturn>> ScheduleInvoke(FlowInstance flowInstance, TParam param, bool? detach, InitialState? initialState)
    {
        var (flowId, storedId) = CreateIds(flowInstance);

        var parentWorkflow = GetAndEnsureParent(detach);
        var scheduledAlreadyParentId = parentWorkflow?.Effect.CreateNextImplicitId();

        if (parentWorkflow != null)
            if (parentWorkflow.Effect.Contains(scheduledAlreadyParentId!))
                return _invocationHelper.CreateInnerScheduled([flowId], parentWorkflow, detach);

        var (created, workflow, disposables, _, _, storageSession) = await PrepareForInvocation(flowId, storedId, param, parentWorkflow?.StoredId, initialState);
        await (parentWorkflow?.Effect.Upsert(scheduledAlreadyParentId!, true, alias: null, flush: false) ?? Task.CompletedTask);

        CurrentFlow._workflow.Value = workflow;
        if (!created)
            return _invocationHelper.CreateInnerScheduled([flowId], parentWorkflow, detach);

        var tcs = new TaskCompletionSource<TReturn>();
        _ = Task.Run(async () =>
        {
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION ***
                    result = await _inner(param, workflow);
                }
                catch (FatalWorkflowException exception)
                {
                    await PersistFailure(storedId, flowId, exception, param, parentWorkflow?.StoredId);
                    tcs.TrySetCanceled();
                    return;
                }
                catch (Exception exception)
                {
                    var fwe = FatalWorkflowException.CreateNonGeneric(flowId, exception);
                    await PersistFailure(storedId, flowId, fwe, param, parentWorkflow?.StoredId);
                    tcs.TrySetCanceled();
                    return;
                }
                finally
                {
                    disposables.Dispose();
                }

                await PersistResultAndEnsureSuccess(
                    storedId,
                    flowId,
                    result,
                    param,
                    parentWorkflow?.StoredId,
                    workflow,
                    storageSession,
                    allowPostponedOrSuspended: true
                );
                if (result.Succeed)
                    tcs.TrySetResult(result.SucceedWithValue!);
                else
                    tcs.TrySetCanceled();
            }
            catch (Exception exception)
            {
                _unhandledExceptionHandler.Invoke(_flowType, exception);
                tcs.TrySetException(exception);
            }
        });

        return _invocationHelper.CreateInnerScheduled([flowId], parentWorkflow, detach, tcs.Task);
    }

    public async Task<InnerScheduled<TReturn>> ScheduleAt(FlowInstance instanceId, TParam param, DateTime scheduleAt, bool? detach)
    {
        var id = new FlowId(_flowType, instanceId);

        var parentWorkflow = GetAndEnsureParent(detach);
        var scheduledAlreadyParentId = parentWorkflow?.Effect.CreateNextImplicitId();

        if (parentWorkflow != null)
            if (parentWorkflow.Effect.Contains(scheduledAlreadyParentId!))
                return _invocationHelper.CreateInnerScheduled([id], parentWorkflow, detach);

        var (_, disposable, _) = await _invocationHelper.PersistFunctionInStore(
            id,
            id.ToStoredId(_storedType),
            instanceId,
            param,
            scheduleAt,
            parentWorkflow?.StoredId,
            _replicaId,
            initialState: null
        );
        await (parentWorkflow?.Effect.Upsert(scheduledAlreadyParentId!, true, alias: null, flush: false) ?? Task.CompletedTask);

        disposable.Dispose();

        return _invocationHelper.CreateInnerScheduled([id], parentWorkflow, detach);
    }

    public async Task<TReturn> Restart(StoredId storedId)
    {
        var (inner, param, humanInstanceId, workflow, disposables, _, _, parent, storageSession) = await PrepareForReInvocation(storedId);
        CurrentFlow._workflow.Value = workflow;
        var flowId = new FlowId(_flowType, humanInstanceId);
        
        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, workflow);
        }
        catch (FatalWorkflowException exception) { await PersistFailure(storedId, flowId, exception, param, parent); throw; }
        catch (Exception exception) { var fwe = FatalWorkflowException.CreateNonGeneric(flowId, exception); await PersistFailure(storedId, flowId, fwe, param, parent); throw fwe; }
        finally{ disposables.Dispose(); }
        
        await PersistResultAndEnsureSuccess(storedId, flowId, result, param, parent, workflow, storageSession);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleRestart(StoredId storedId)
    {
        var (inner, param, humanInstanceId, workflow, disposables, _, _, parent, storageSession) = await PrepareForReInvocation(storedId);
        var flowId = new FlowId(_flowType, humanInstanceId);
        
        _ = Task.Run(async () =>
        {
            CurrentFlow._workflow.Value = workflow;
            
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param, workflow);
                }
                catch (FatalWorkflowException exception) { await PersistFailure(storedId, flowId, exception, param, parent); throw; }
                catch (Exception exception) { var fwe = FatalWorkflowException.CreateNonGeneric(flowId, exception); await PersistFailure(storedId, flowId, fwe, param, parent); throw fwe; }
                finally{ disposables.Dispose(); }
                
                await PersistResultAndEnsureSuccess(storedId, flowId, result, param, parent, workflow, storageSession, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
        });
    }
    
    internal async Task ScheduleRestart(StoredId storedId, RestartedFunction rf, Action onCompletion)
    {
        var (inner, param, humanInstanceId, workflow, disposables, _, _, parent, storageSession) = await PrepareForReInvocation(storedId, rf);
        var flowId = new FlowId(_flowType, humanInstanceId);
        
        _ = Task.Run(async () =>
        {
            CurrentFlow._workflow.Value = workflow;
            
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param, workflow);
                }
                catch (FatalWorkflowException exception) { await PersistFailure(storedId, flowId, exception, param, parent); throw; }
                catch (Exception exception) { var fwe = FatalWorkflowException.CreateNonGeneric(flowId, exception); await PersistFailure(storedId, flowId, fwe, param, parent); throw fwe; }
                finally { disposables.Dispose(); }
                
                await PersistResultAndEnsureSuccess(storedId, flowId, result, param, parent, workflow, storageSession, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
            finally{ onCompletion(); }
        });
    }
    
    private async Task<PreparedInvocation> PrepareForInvocation(FlowId flowId, StoredId storedId, TParam param, StoredId? parent, InitialState? initialState)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        var success = false;
        try
        {
            var (persisted, runningFunction, storageSession) =
                await _invocationHelper.PersistFunctionInStore(
                    flowId,
                    storedId,
                    flowId.Instance.Value,
                    param,
                    scheduleAt: null,
                    parent,
                    _replicaId,
                    initialState
                );
            disposables.Add(runningFunction);
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            success = persisted;

            var flowTimeouts = new FlowTimeouts(_flowsTimeoutManager, storedId);
            var effect = _invocationHelper.CreateEffect(
                storedId,
                flowId,
                initialState == null ? [] : _invocationHelper.MapInitialEffects(initialState.Effects, flowId),
                flowTimeouts,
                storageSession
            );

            var correlations = _invocationHelper.CreateCorrelations(flowId);
            var semaphores = _invocationHelper.CreateSemaphores(storedId, effect);
            var queueManager = _invocationHelper.CreateQueueManager(flowId, storedId, effect, flowTimeouts, _unhandledExceptionHandler, _flowsTimeoutManager);
            disposables.Add(queueManager);
            var messageWriter = _invocationHelper.CreateMessageWriter(storedId);
            var workflow = new Workflow(flowId, storedId, effect, _utilities, correlations, semaphores, queueManager, _invocationHelper.UtcNow, messageWriter);

            return new PreparedInvocation(
                persisted,
                workflow,
                Disposable.Combine(disposables),
                queueManager,
                flowTimeouts
            );
        }
        catch (Exception)
        {
            success = false;
            throw;
        }
        finally
        {
            if (!success) Disposable.Combine(disposables).Dispose();
        }
    }
    private record PreparedInvocation(bool Persisted, Workflow Workflow, IDisposable Disposables, QueueManager QueueManager, FlowTimeouts FlowTimeouts, IStorageSession? StorageSession = null);

    private async Task<PreparedReInvocation> PrepareForReInvocation(StoredId storedId)
    {
        var restartedFunction = await _invocationHelper.RestartFunction(storedId);
        if (restartedFunction == null)
            throw UnexpectedStateException.ConcurrentModification(storedId);

        return await PrepareForReInvocation(storedId, restartedFunction);
    }

    private async Task<PreparedReInvocation> PrepareForReInvocation(StoredId storedId, RestartedFunction restartedFunction)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        try
        {
            var (flowId, param, effects, storedMessages, runningFunction, parent, storageSession) = 
                await _invocationHelper.PrepareForReInvocation(storedId, restartedFunction);
            disposables.Add(runningFunction);
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            
            var flowTimeouts = new FlowTimeouts(_flowsTimeoutManager, storedId);
            var effect = _invocationHelper.CreateEffect(storedId, flowId, effects, flowTimeouts, storageSession);

            var correlations = _invocationHelper.CreateCorrelations(flowId);
            var semaphores = _invocationHelper.CreateSemaphores(storedId, effect);
            var queueManager = _invocationHelper.CreateQueueManager(flowId, storedId, effect, flowTimeouts, _unhandledExceptionHandler, _flowsTimeoutManager);
            disposables.Add(queueManager);
            var messageWriter = _invocationHelper.CreateMessageWriter(storedId);

            var workflow = new Workflow(
                flowId,
                storedId,
                effect,
                _utilities,
                correlations,
                semaphores,
                queueManager,
                _invocationHelper.UtcNow,
                messageWriter
            );

            return new PreparedReInvocation(
                _inner,
                param!, //todo implement param null case
                flowId.Instance.Value,
                workflow,
                Disposable.Combine(disposables),
                queueManager,
                flowTimeouts,
                parent,
                storageSession
            );
        }
        catch(Exception)
        {
            Disposable.Combine(disposables).Dispose();
            throw;
        }
    }

    private record PreparedReInvocation(
        Func<TParam, Workflow, Task<Result<TReturn>>> Inner,
        TParam Param,
        string HumanInstanceId,
        Workflow Workflow,
        IDisposable Disposables,
        QueueManager QueueManager,
        FlowTimeouts FlowTimeouts,
        StoredId? Parent,
        IStorageSession? StorageSession
    );

    private async Task PersistFailure(StoredId storedId, FlowId flowId, FatalWorkflowException exception, TParam param, StoredId? parent)
    {
        await _invocationHelper.PublishCompletionMessageToParent(parent, childId: flowId, result: Fail.WithException(exception));
        await _invocationHelper.PersistFailure(storedId, exception, param);
    }

    private async Task PersistResultAndEnsureSuccess(StoredId storedId, FlowId flowId, Result<TReturn> result, TParam param, StoredId? parent, Workflow workflow, IStorageSession? storageSession, bool allowPostponedOrSuspended = false)
    {
        await workflow.Effect.Flush();
        await _invocationHelper.PublishCompletionMessageToParent(parent, childId: flowId, result);
        
        var outcome = await _invocationHelper.PersistResult(storedId, result, param, storageSession);
        switch (outcome)
        {
            case PersistResultOutcome.Failed:
                throw UnexpectedStateException.ConcurrentModification(storedId);
            case PersistResultOutcome.Success:
                InvocationHelper<TParam, TReturn>.EnsureSuccess(flowId, result, allowPostponedOrSuspended);
                break;
            case PersistResultOutcome.Reschedule:
                await _invocationHelper.Reschedule(storedId, param);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private (FlowId, StoredId) CreateIds(FlowInstance instanceId)
        => CreateIds(instanceId.Value);
    private (FlowId, StoredId) CreateIds(string instanceId)
        => (new FlowId(_flowType, instanceId), StoredId.Create(_storedType, instanceId));

    private Workflow? GetAndEnsureParent(bool? detach) => _invocationHelper.GetAndEnsureParent(detach);
}