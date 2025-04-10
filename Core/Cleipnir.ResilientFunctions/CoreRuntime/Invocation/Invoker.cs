using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers.Disposables;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Invoker<TParam, TReturn> 
{
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly Func<TParam, Workflow, Task<Result<TReturn>>> _inner;
    
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly Utilities _utilities;

    internal Invoker(
        FlowType flowType, StoredType storedType,
        Func<TParam, Workflow, Task<Result<TReturn>>> inner,
        InvocationHelper<TParam, TReturn> invocationHelper,
        UnhandledExceptionHandler unhandledExceptionHandler,
        Utilities utilities
    )
    {
        _flowType = flowType;
        _storedType = storedType;
        _inner = inner;
        _invocationHelper = invocationHelper;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _utilities = utilities;
    }

    public async Task<TReturn> Invoke(FlowInstance instance, TParam param, InitialState? initialState = null)
    {
        var (flowId, storedId) = CreateIds(instance);
        var (created, workflow, disposables) = await PrepareForInvocation(flowId, storedId, param, parent: null, initialState);
        CurrentFlow._workflow.Value = workflow;
        if (!created) return await WaitForFunctionResult(flowId, storedId, maxWait: null);

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await _inner(param, workflow);
        }
        catch (Exception exception)
        {
            var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(flowId, exception); 
            await PersistFailure(storedId, flowId, fatalWorkflowException, param, parent: null); 
            throw FatalWorkflowException.CreateNonGeneric(flowId, exception);
        }
        finally{ disposables.Dispose(); }

        await PersistResultAndEnsureSuccess(storedId, flowId, result, param, parent: null);
        return result.SucceedWithValue!;
    }

    public async Task<InnerScheduled<TReturn>> ScheduleInvoke(FlowInstance flowInstance, TParam param, bool? detach, InitialState? initialState)
    {
        var parent = GetAndEnsureParent(detach);
        var (flowId, storedId) = CreateIds(flowInstance);

        if (parent != null)
        {
            var marked = await parent.Effect.Mark($"{flowId}_Scheduled");
            if (!marked)
                return _invocationHelper.CreateInnerScheduled([flowId], parent, detach);    
        }
        
        var (created, workflow, disposables) = await PrepareForInvocation(flowId, storedId, param, parent?.StoredId, initialState);
        CurrentFlow._workflow.Value = workflow;
        if (!created) 
            return _invocationHelper.CreateInnerScheduled([flowId], parent, detach);

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
                catch (FatalWorkflowException exception) { await PersistFailure(storedId, flowId, exception, param, parent?.StoredId); throw; }
                catch (Exception exception) { var fwe = FatalWorkflowException.CreateNonGeneric(flowId, exception); await PersistFailure(storedId, flowId, fwe, param, parent?.StoredId); throw fwe; }
                finally{ disposables.Dispose(); }

                await PersistResultAndEnsureSuccess(storedId, flowId, result, param, parent?.StoredId, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
        });

        return _invocationHelper.CreateInnerScheduled([flowId], parent, detach);
    }

    public async Task<InnerScheduled<TReturn>> ScheduleAt(FlowInstance instanceId, TParam param, DateTime scheduleAt, bool? detach)
    {
        var parent = GetAndEnsureParent(detach);
        var id = new FlowId(_flowType, instanceId);
        if (parent != null)
        {
            var marked = await parent.Effect.Mark($"{id}_Scheduled");
            if (!marked)
                return _invocationHelper.CreateInnerScheduled([id], parent, detach);
        }

        var (_, disposable) = await _invocationHelper.PersistFunctionInStore(
            id,
            id.ToStoredId(_storedType),
            instanceId,
            param,
            scheduleAt,
            parent?.StoredId,
            initialState: null
        );

        disposable.Dispose();

        return _invocationHelper.CreateInnerScheduled([id], parent, detach);
    }

    public async Task<TReturn> Restart(StoredInstance instanceId, int expectedEpoch)
    {
        var storedId = new StoredId(_storedType, instanceId);
        var (inner, param, humanInstanceId, workflow, epoch, disposables, parent) = await PrepareForReInvocation(storedId, expectedEpoch);
        CurrentFlow._workflow.Value = workflow;
        var flowId = new FlowId(_flowType, humanInstanceId);
        
        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, workflow);
        }
        catch (FatalWorkflowException exception) { await PersistFailure(storedId, flowId, exception, param, parent, epoch); throw; }
        catch (Exception exception) { var fwe = FatalWorkflowException.CreateNonGeneric(flowId, exception); await PersistFailure(storedId, flowId, fwe, param, parent, epoch); throw fwe; }
        finally{ disposables.Dispose(); }
        
        await PersistResultAndEnsureSuccess(storedId, flowId, result, param, parent, epoch);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleRestart(StoredInstance instance, int expectedEpoch)
    {
        var storedId = new StoredId(_storedType, instance);
        var (inner, param, humanInstanceId, workflow, epoch, disposables, parent) = await PrepareForReInvocation(storedId, expectedEpoch);
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
                catch (FatalWorkflowException exception) { await PersistFailure(storedId, flowId, exception, param, parent, epoch); throw; }
                catch (Exception exception) { var fwe = FatalWorkflowException.CreateNonGeneric(flowId, exception); await PersistFailure(storedId, flowId, fwe, param, parent, epoch); throw fwe; }
                finally{ disposables.Dispose(); }
                
                await PersistResultAndEnsureSuccess(storedId, flowId, result, param, parent, epoch, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
        });
    }
    
    private async Task<TReturn> WaitForFunctionResult(FlowId flowId, StoredId storedId, TimeSpan? maxWait)
        => await _invocationHelper.WaitForFunctionResult(flowId, storedId, allowPostponedAndSuspended: false, maxWait);

    internal async Task ScheduleRestart(StoredInstance instance, RestartedFunction rf, Action onCompletion)
    {
        var storedId = new StoredId(_storedType, instance);
        var (inner, param, humanInstanceId, workflow, epoch, disposables, parent) = await PrepareForReInvocation(storedId, rf);
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
                catch (FatalWorkflowException exception) { await PersistFailure(storedId, flowId, exception, param, parent, epoch); throw; }
                catch (Exception exception) { var fwe = FatalWorkflowException.CreateNonGeneric(flowId, exception); await PersistFailure(storedId, flowId, fwe, param, parent, epoch); throw fwe; }
                finally { disposables.Dispose(); }
                
                await PersistResultAndEnsureSuccess(storedId, flowId, result, param, parent, epoch, allowPostponedOrSuspended: true);
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
            var (persisted, runningFunction) =
                await _invocationHelper.PersistFunctionInStore(
                    flowId,
                    storedId,
                    flowId.Instance.Value,
                    param,
                    scheduleAt: null,
                    parent,
                    initialState
                );
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(storedId, epoch: 0));
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            success = persisted;

            var (effect, states) = _invocationHelper.CreateEffectAndStates(
                storedId,
                flowId,
                initialState == null ? [] : _invocationHelper.MapInitialEffects(initialState.Effects, flowId)
            );
            var messages = _invocationHelper.CreateMessages(
                flowId,
                storedId,
                ScheduleRestart, 
                isWorkflowRunning: () => !isWorkflowRunningDisposable.Disposed,
                effect,
                initialState == null ? [] : _invocationHelper.MapInitialMessages(initialState.Messages)
            );
            
            var correlations = _invocationHelper.CreateCorrelations(flowId);
            var semaphores = _invocationHelper.CreateSemaphores(storedId, effect);
            var workflow = new Workflow(flowId, storedId, messages, effect, states, _utilities, correlations, semaphores, _invocationHelper.UtcNow);

            return new PreparedInvocation(
                persisted,
                workflow,
                Disposable.Combine(disposables)
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
    private record PreparedInvocation(bool Persisted, Workflow Workflow, IDisposable Disposables);

    private async Task<PreparedReInvocation> PrepareForReInvocation(StoredId storedId, int expectedEpoch)
    {
        var restartedFunction = await _invocationHelper.RestartFunction(storedId, expectedEpoch);
        if (restartedFunction == null)
            throw UnexpectedStateException.EpochMismatch(storedId);

        return await PrepareForReInvocation(storedId, restartedFunction);
    }

    private async Task<PreparedReInvocation> PrepareForReInvocation(StoredId storedId, RestartedFunction restartedFunction)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        try
        {
            var (flowId, param, epoch, effects, storedMessages, runningFunction, parent) = 
                await _invocationHelper.PrepareForReInvocation(storedId, restartedFunction);
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(storedId, epoch));
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            
            var (effect, states) = _invocationHelper.CreateEffectAndStates(storedId, flowId, effects);
            var messages = _invocationHelper.CreateMessages(
                flowId,
                storedId,
                ScheduleRestart,
                isWorkflowRunning: () => !isWorkflowRunningDisposable.Disposed,
                effect,
                storedMessages
            );
            
            var correlations = _invocationHelper.CreateCorrelations(flowId);
            var semaphores = _invocationHelper.CreateSemaphores(storedId, effect);
            
            var workflow = new Workflow(
                flowId,
                storedId,
                messages,
                effect,
                states,
                _utilities,
                correlations,
                semaphores,
                _invocationHelper.UtcNow
            );

            return new PreparedReInvocation(
                _inner,
                param!, //todo implement param null case
                flowId.Instance.Value,
                workflow,
                epoch,
                Disposable.Combine(disposables),
                parent
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
        int Epoch,
        IDisposable Disposables,
        StoredId? Parent
    );

    private async Task PersistFailure(StoredId storedId, FlowId flowId, FatalWorkflowException exception, TParam param, StoredId? parent, int expectedEpoch = 0)
    {
        await _invocationHelper.PublishCompletionMessageToParent(parent, childId: flowId, result: Fail.WithException(exception));
        await _invocationHelper.PersistFailure(storedId, flowId, exception, param, parent, expectedEpoch);
    }

    private async Task PersistResultAndEnsureSuccess(StoredId storedId, FlowId flowId, Result<TReturn> result, TParam param, StoredId? parent, int expectedEpoch = 0, bool allowPostponedOrSuspended = false)
    {
        await _invocationHelper.PublishCompletionMessageToParent(parent, childId: flowId, result);
        
        var outcome = await _invocationHelper.PersistResult(storedId, flowId, result, param, parent, expectedEpoch);
        switch (outcome)
        {
            case PersistResultOutcome.Failed:
                throw UnexpectedStateException.ConcurrentModification(storedId);
            case PersistResultOutcome.Success:
                InvocationHelper<TParam, TReturn>.EnsureSuccess(flowId, result, allowPostponedOrSuspended);
                break;
            case PersistResultOutcome.Reschedule:
            {
                try
                {
                    await ScheduleRestart(flowId.Instance.ToStoredInstance(), expectedEpoch);
                } catch (UnexpectedStateException) {} //allow this exception - the invocation has been surpassed by other execution
                InvocationHelper<TParam, TReturn>.EnsureSuccess(flowId, result, allowPostponedOrSuspended);
                break;
            }
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private (FlowId, StoredId) CreateIds(FlowInstance instanceId)
        => CreateIds(instanceId.Value);
    private (FlowId, StoredId) CreateIds(string instanceId)
        => (new FlowId(_flowType, instanceId), new StoredId(_storedType, instanceId.ToStoredInstance()));

    private Workflow? GetAndEnsureParent(bool? detach) => _invocationHelper.GetAndEnsureParent(detach);
}