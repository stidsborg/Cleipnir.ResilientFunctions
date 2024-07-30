using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers.Disposables;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Invoker<TParam, TReturn> 
{
    private readonly FlowType _flowType;
    private readonly Func<TParam, Workflow, Task<Result<TReturn>>> _inner;
    
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly Utilities _utilities;

    internal Invoker(
        FlowType flowType,
        Func<TParam, Workflow, Task<Result<TReturn>>> inner,
        InvocationHelper<TParam, TReturn> invocationHelper,
        UnhandledExceptionHandler unhandledExceptionHandler,
        Utilities utilities
    )
    {
        _flowType = flowType;
        _inner = inner;
        _invocationHelper = invocationHelper;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _utilities = utilities;
    }

    public async Task<TReturn> Invoke(string flowInstance, TParam param)
    {
        var functionId = new FlowId(_flowType, flowInstance);
        (var created, var workflow, var disposables) = await PrepareForInvocation(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await _inner(param, workflow);
        }
        catch (Exception exception) { await PersistFailure(functionId, exception, param, workflow); throw; }
        finally{ disposables.Dispose(); }

        await PersistResultAndEnsureSuccess(functionId, result, param, workflow);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleInvoke(string flowInstance, TParam param)
    {
        var functionId = new FlowId(_flowType, flowInstance);
        (var created, var workflow, var disposables) = await PrepareForInvocation(functionId, param);
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
                catch (Exception exception) { await PersistFailure(functionId, exception, param, workflow); throw; }
                finally{ disposables.Dispose(); }

                await PersistResultAndEnsureSuccess(functionId, result, param, workflow, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
        });
    }
    
    public async Task ScheduleAt(string instanceId, TParam param, DateTime scheduleAt)
    {
        if (scheduleAt.ToUniversalTime() <= DateTime.UtcNow)
        {
            await ScheduleInvoke(instanceId, param);
            return;
        }

        var functionId = new FlowId(_flowType, instanceId);
        var (_, disposable) = await _invocationHelper.PersistFunctionInStore(
            functionId,
            param,
            scheduleAt
        );

        disposable.Dispose();
    }

    public async Task<TReturn> Restart(string instanceId, int expectedEpoch)
    {
        var functionId = new FlowId(_flowType, instanceId);
        var (inner, param, workflow, epoch, disposables) = await PrepareForReInvocation(functionId, expectedEpoch);

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, workflow);
        }
        catch (Exception exception) { await PersistFailure(functionId, exception, param, workflow, epoch); throw; }
        finally{ disposables.Dispose(); }
        
        await PersistResultAndEnsureSuccess(functionId, result, param, workflow, epoch);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleRestart(string instanceId, int expectedEpoch)
    {
        var functionId = new FlowId(_flowType, instanceId);
        var (inner, param, workflow, epoch, disposables) = await PrepareForReInvocation(functionId, expectedEpoch);

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
                catch (Exception exception) { await PersistFailure(functionId, exception, param, workflow, epoch); throw; }
                finally{ disposables.Dispose(); }
                
                await PersistResultAndEnsureSuccess(functionId, result, param, workflow, epoch, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
        });
    }
    
    private async Task<TReturn> WaitForFunctionResult(FlowId flowId)
        => await _invocationHelper.WaitForFunctionResult(flowId, allowPostponedAndSuspended: false);

    internal async Task ScheduleRestart(FlowInstance instance, RestartedFunction rf, Action onCompletion)
    {
        var functionId = new FlowId(_flowType, instance);
        var (inner, param, workflow, epoch, disposables) = await PrepareForReInvocation(functionId, rf);

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
                catch (Exception exception) { await PersistFailure(functionId, exception, param, workflow, epoch); throw; }
                finally { disposables.Dispose(); }
                
                await PersistResultAndEnsureSuccess(functionId, result, param, workflow, epoch, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_flowType, exception); }
            finally{ onCompletion(); }
        });
    }
    
    private async Task<PreparedInvocation> PrepareForInvocation(FlowId flowId, TParam param)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        var success = false;
        try
        {
            var (persisted, runningFunction) =
                await _invocationHelper.PersistFunctionInStore(
                    flowId,
                    param,
                    scheduleAt: null
                );
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(flowId, epoch: 0));
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            success = persisted;
            var messages = await _invocationHelper.CreateMessages(
                flowId, 
                ScheduleRestart, 
                isWorkflowRunning: () => !isWorkflowRunningDisposable.Disposed,
                sync: false
            );
            
            var effect = _invocationHelper.CreateEffect(flowId);
            var states = await _invocationHelper.CreateStates(flowId, defaultState: null, sync: false);
            var correlations = await _invocationHelper.CreateCorrelations(flowId, sync: false);
            var workflow = new Workflow(flowId, messages, effect, states, _utilities, correlations);

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

    private async Task<PreparedReInvocation> PrepareForReInvocation(FlowId flowId, int expectedEpoch)
    {
        var restartedFunction = await _invocationHelper.RestartFunction(flowId, expectedEpoch);
        if (restartedFunction == null)
            throw new UnexpectedFunctionState(flowId, $"Function '{flowId}' did not have expected epoch '{expectedEpoch}' on re-invocation");

        return await PrepareForReInvocation(flowId, restartedFunction);
    }

    private async Task<PreparedReInvocation> PrepareForReInvocation(FlowId flowId, RestartedFunction restartedFunction)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        try
        {
            var (param, epoch, defaultState, runningFunction) = 
                await _invocationHelper.PrepareForReInvocation(flowId, restartedFunction);
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(flowId, epoch));
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            
            var messagesTask = Task.Run(() => _invocationHelper.CreateMessages(
                flowId, 
                ScheduleRestart,
                isWorkflowRunning: () => !isWorkflowRunningDisposable.Disposed,
                sync: true));
            var statesTask = Task.Run(() => _invocationHelper.CreateStates(flowId, defaultState, sync: true));
            var correlationsTask = Task.Run(() => _invocationHelper.CreateCorrelations(flowId, sync: true));
            var workflow = new Workflow(
                flowId,
                await messagesTask,
                _invocationHelper.CreateEffect(flowId),
                await statesTask,
                _utilities,
                await correlationsTask
            );

            return new PreparedReInvocation(
                _inner,
                param!, //todo implement param null case 
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
        Workflow Workflow,
        int Epoch,
        IDisposable Disposables
    );

    private async Task PersistFailure(FlowId flowId, Exception exception, TParam param, Workflow workflow, int expectedEpoch = 0)
        => await _invocationHelper.PersistFailure(flowId, exception, param, workflow.States.SerializeDefaultState(), expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FlowId flowId, Result<TReturn> result, TParam param, Workflow workflow, int expectedEpoch = 0, bool allowPostponedOrSuspended = false)
    {
        var outcome = await _invocationHelper.PersistResult(flowId, result, param, workflow.States.SerializeDefaultState(), expectedEpoch);
        switch (outcome)
        {
            case PersistResultOutcome.Failed:
                throw new ConcurrentModificationException(flowId);
            case PersistResultOutcome.Success:
                InvocationHelper<TParam, TReturn>.EnsureSuccess(flowId, result, allowPostponedOrSuspended);
                break;
            case PersistResultOutcome.Reschedule:
            {
                try
                {
                    await ScheduleRestart(flowId.Instance.Value, expectedEpoch);
                } catch (UnexpectedFunctionState) {} //allow this exception - the invocation has been surpassed by other execution
                InvocationHelper<TParam, TReturn>.EnsureSuccess(flowId, result, allowPostponedOrSuspended);
                break;
            }
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}