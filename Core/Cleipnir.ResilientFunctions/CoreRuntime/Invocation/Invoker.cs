using System;
using System.Collections.Generic;
using System.Threading;
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

    public async Task<TReturn> Invoke(FlowInstance instance, TParam param)
    {
        var (flowId, storedId) = CreateIds(instance);
        CurrentFlow._id.Value = storedId;
        var (created, workflow, disposables) = await PrepareForInvocation(flowId, storedId, param, parent: null);
        if (!created) return await WaitForFunctionResult(flowId, storedId);

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await _inner(param, workflow);
        }
        catch (Exception exception) { await PersistFailure(storedId, exception, param, workflow); throw; }
        finally{ disposables.Dispose(); }

        await PersistResultAndEnsureSuccess(storedId, flowId, result, param, workflow);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleInvoke(FlowInstance flowInstance, TParam param, bool suspendUntilCompletion = false)
    {
        var parent = suspendUntilCompletion ? CurrentFlow.StoredId : null;
        var (flowId, storedId) = CreateIds(flowInstance);
        CurrentFlow._id.Value = storedId;
        (var created, var workflow, var disposables) = await PrepareForInvocation(flowId, storedId, param, parent);
        if (!created) return;

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
                catch (Exception exception) { await PersistFailure(storedId, exception, param, workflow); throw; }
                finally{ disposables.Dispose(); }

                await PersistResultAndEnsureSuccess(storedId, flowId, result, param, workflow, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
        });
    }
    
    public async Task ScheduleAt(FlowInstance instanceId, TParam param, DateTime scheduleAt, bool suspendUntilCompletion = false)
    {
        if (scheduleAt.ToUniversalTime() <= DateTime.UtcNow)
        {
            await ScheduleInvoke(instanceId, param);
            return;
        }

        var parent = suspendUntilCompletion
            ? CurrentFlow.StoredId
            : null;
        
        var id = new FlowId(_flowType, instanceId);
        var (_, disposable) = await _invocationHelper.PersistFunctionInStore(
            id.ToStoredId(_storedType),
            instanceId,
            param,
            scheduleAt,
            parent
        );

        disposable.Dispose();
    }

    public async Task<TReturn> Restart(StoredInstance instanceId, int expectedEpoch)
    {
        var storedId = new StoredId(_storedType, instanceId);
        CurrentFlow._id.Value = storedId;
        var (inner, param, humanInstanceId, workflow, epoch, disposables) = await PrepareForReInvocation(storedId, expectedEpoch);
        var flowId = new FlowId(_flowType, humanInstanceId);
        
        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, workflow);
        }
        catch (Exception exception) { await PersistFailure(storedId, exception, param, workflow, epoch); throw; }
        finally{ disposables.Dispose(); }
        
        await PersistResultAndEnsureSuccess(storedId, flowId, result, param, workflow, epoch);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleRestart(StoredInstance instance, int expectedEpoch)
    {
        var storedId = new StoredId(_storedType, instance);
        CurrentFlow._id.Value = storedId;
        var (inner, param, humanInstanceId, workflow, epoch, disposables) = await PrepareForReInvocation(storedId, expectedEpoch);
        var flowId = new FlowId(_flowType, humanInstanceId);
        
        _ = Task.Run(async () =>
        {
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param, workflow);
                }
                catch (Exception exception) { await PersistFailure(storedId, exception, param, workflow, epoch); throw; }
                finally{ disposables.Dispose(); }
                
                await PersistResultAndEnsureSuccess(storedId, flowId, result, param, workflow, epoch, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
        });
    }
    
    private async Task<TReturn> WaitForFunctionResult(FlowId flowId, StoredId storedId)
        => await _invocationHelper.WaitForFunctionResult(flowId, storedId, allowPostponedAndSuspended: false);

    internal async Task ScheduleRestart(StoredInstance instance, RestartedFunction rf, Action onCompletion)
    {
        var storedId = new StoredId(_storedType, instance);
        CurrentFlow._id.Value = storedId;
        var (inner, param, humanInstanceId, workflow, epoch, disposables) = await PrepareForReInvocation(storedId, rf);
        var flowId = new FlowId(_flowType, humanInstanceId);
        
        _ = Task.Run(async () =>
        {
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param, workflow);
                }
                catch (Exception exception) { await PersistFailure(storedId, exception, param, workflow, epoch); throw; }
                finally { disposables.Dispose(); }
                
                await PersistResultAndEnsureSuccess(storedId, flowId, result, param, workflow, epoch, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
            finally{ onCompletion(); }
        });
    }
    
    private async Task<PreparedInvocation> PrepareForInvocation(FlowId flowId, StoredId storedId, TParam param, StoredId? parent)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        var success = false;
        try
        {
            var (persisted, runningFunction) =
                await _invocationHelper.PersistFunctionInStore(
                    storedId,
                    flowId.Instance.Value,
                    param,
                    scheduleAt: null,
                    parent
                );
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(storedId, flowId, epoch: 0));
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            success = persisted;
            
            var messages = _invocationHelper.CreateMessages(
                storedId,
                ScheduleRestart, 
                isWorkflowRunning: () => !isWorkflowRunningDisposable.Disposed
            );
            
            var (effect, states) = _invocationHelper.CreateEffectAndStates(storedId, anyEffects: false);
            var correlations = _invocationHelper.CreateCorrelations(flowId);
            var workflow = new Workflow(flowId, storedId, messages, effect, states, _utilities, correlations);

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
            var (flowId, param, epoch, runningFunction) = 
                await _invocationHelper.PrepareForReInvocation(storedId, restartedFunction);
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(storedId, flowId, epoch));
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            
            var messages = _invocationHelper.CreateMessages(
                storedId,
                ScheduleRestart,
                isWorkflowRunning: () => !isWorkflowRunningDisposable.Disposed
            );

            var (effect, states) = _invocationHelper.CreateEffectAndStates(storedId, anyEffects: true);
            var correlations = _invocationHelper.CreateCorrelations(flowId);
          
            var workflow = new Workflow(
                flowId,
                storedId,
                messages,
                effect,
                states,
                _utilities,
                correlations
            );

            return new PreparedReInvocation(
                _inner,
                param!, //todo implement param null case
                flowId.Instance.Value,
                workflow,
                epoch,
                Disposable.Combine(disposables)
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
        IDisposable Disposables
    );

    private async Task PersistFailure(StoredId storedId, Exception exception, TParam param, Workflow workflow, int expectedEpoch = 0)
        => await _invocationHelper.PersistFailure(storedId, exception, param, expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(StoredId storedId, FlowId flowId, Result<TReturn> result, TParam param, Workflow workflow, int expectedEpoch = 0, bool allowPostponedOrSuspended = false)
    {
        var outcome = await _invocationHelper.PersistResult(storedId, result, param, expectedEpoch);
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
                    await ScheduleRestart(flowId.Instance.Value, expectedEpoch);
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
}