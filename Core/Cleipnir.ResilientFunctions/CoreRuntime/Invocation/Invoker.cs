using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers.Disposables;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Invoker<TParam, TState, TReturn> 
    where TParam : notnull 
    where TState : WorkflowState, new() 
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam,TState,Workflow,Task<Result<TReturn>>> _inner;
    
    private readonly InvocationHelper<TParam, TState, TReturn> _invocationHelper;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly Utilities _utilities;
    private readonly Func<FunctionId, MessageWriter> _messageWriterFunc;

    internal Invoker(
        FunctionTypeId functionTypeId,
        Func<TParam, TState, Workflow, Task<Result<TReturn>>> inner,
        InvocationHelper<TParam, TState, TReturn> invocationHelper,
        UnhandledExceptionHandler unhandledExceptionHandler,
        Utilities utilities,
        Func<FunctionId, MessageWriter> messageWriterFunc
    )
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _invocationHelper = invocationHelper;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _utilities = utilities;
        _messageWriterFunc = messageWriterFunc;
    }

    public async Task<TReturn> Invoke(string functionInstanceId, TParam param, TState? state = null)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        (var created, state, var workflow, var disposables) = await PrepareForInvocation(functionId, param, state);
        if (!created) return await WaitForFunctionResult(functionId);

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await _inner(param, state, workflow);
        }
        catch (Exception exception) { await PersistFailure(functionId, exception, param, state); throw; }
        finally{ disposables.Dispose(); }

        await PersistResultAndEnsureSuccess(functionId, result, param, state);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleInvoke(string functionInstanceId, TParam param, TState? state)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        (var created, state, var workflow, var disposables) = await PrepareForInvocation(functionId, param, state);
        if (!created) return;

        _ = Task.Run(async () =>
        {
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _inner(param, state, workflow);
                }
                catch (Exception exception) { await PersistFailure(functionId, exception, param, state); throw; }
                finally{ disposables.Dispose(); }

                await PersistResultAndEnsureSuccess(functionId, result, param, state, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
        });
    }
    
    public async Task ScheduleAt(string instanceId, TParam param, DateTime scheduleAt, TState? state)
    {
        if (scheduleAt.ToUniversalTime() <= DateTime.UtcNow)
        {
            await ScheduleInvoke(instanceId, param, state);
            return;
        }

        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (_, disposable) = await _invocationHelper.PersistFunctionInStore(
            functionId,
            param,
            state ?? new TState(),
            scheduleAt
        );

        disposable.Dispose();
    }

    public async Task<TReturn> ReInvoke(string instanceId, int expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (inner, param, state, workflow, epoch, disposables) = await PrepareForReInvocation(functionId, expectedEpoch);

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, state, workflow);
        }
        catch (Exception exception) { await PersistFailure(functionId, exception, param, state, epoch); throw; }
        finally{ disposables.Dispose(); }
        
        await PersistResultAndEnsureSuccess(functionId, result, param, state, epoch);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleReInvoke(string instanceId, int expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (inner, param, state, workflow, epoch, disposables) = await PrepareForReInvocation(functionId, expectedEpoch);

        _ = Task.Run(async () =>
        {
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param, state, workflow);
                }
                catch (Exception exception) { await PersistFailure(functionId, exception, param, state, epoch); throw; }
                finally{ disposables.Dispose(); }
                
                await PersistResultAndEnsureSuccess(functionId, result, param, state, epoch, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
        });
    }
    
    private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        => await _invocationHelper.WaitForFunctionResult(functionId, allowPostponedAndSuspended: false);

    private async Task<PreparedInvocation> PrepareForInvocation(FunctionId functionId, TParam param, TState? state)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        var success = false;
        try
        {
            state ??= new TState();
            _invocationHelper.InitializeState(functionId, param, state, epoch: 0);

            var (persisted, runningFunction) =
                await _invocationHelper.PersistFunctionInStore(
                    functionId,
                    param,
                    state,
                    scheduleAt: null
                );
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(functionId, epoch: 0));
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            success = persisted;
            var messages = await _invocationHelper.CreateMessages(
                functionId, 
                ScheduleReInvoke, 
                isWorkflowRunning: () => !isWorkflowRunningDisposable.Disposed,
                sync: false
            );
            
            var effect = await _invocationHelper.CreateEffect(functionId, sync: false);
            var workflow = new Workflow(functionId, messages, effect, _utilities, _messageWriterFunc);

            return new PreparedInvocation(
                persisted,
                state,
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
    private record PreparedInvocation(bool Persisted, TState State, Workflow Workflow, IDisposable Disposables);

    private async Task<PreparedReInvocation> PrepareForReInvocation(FunctionId functionId, int expectedEpoch)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        try
        {
            var (param, epoch, state, interruptCount, runningFunction) = 
                await _invocationHelper.PrepareForReInvocation(functionId, expectedEpoch);
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(functionId, epoch));
            var isWorkflowRunningDisposable = new PropertyDisposable();
            disposables.Add(isWorkflowRunningDisposable);
            
            var messagesTask = Task.Run(() => _invocationHelper.CreateMessages(
                functionId, 
                ScheduleReInvoke,
                isWorkflowRunning: () => !isWorkflowRunningDisposable.Disposed,
                sync: true));
            var activitiesTask = Task.Run(() => _invocationHelper.CreateEffect(functionId, sync: true));
            var workflow = new Workflow(functionId, await messagesTask, await activitiesTask, _utilities, _messageWriterFunc);

            return new PreparedReInvocation(
                _inner,
                param,
                state,
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
    private record PreparedReInvocation(Func<TParam, TState, Workflow, Task<Result<TReturn>>> Inner, TParam Param, TState State, Workflow Workflow, int Epoch, IDisposable Disposables);

    private async Task PersistFailure(FunctionId functionId, Exception exception, TParam param, TState state, int expectedEpoch = 0)
        => await _invocationHelper.PersistFailure(functionId, exception, param, state, expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FunctionId functionId, Result<TReturn> result, TParam param, TState state, int expectedEpoch = 0, bool allowPostponedOrSuspended = false)
    {
        if (result.Succeed && result.SucceedWithValue is Task)
        {
            var serializationException = new SerializationException("Unable to serialize result of Task-type");
            await _invocationHelper.PersistFailure(functionId, serializationException, param, state, expectedEpoch);
            throw serializationException;
        }
            
        var outcome = await _invocationHelper.PersistResult(functionId, result, param, state, expectedEpoch);
        switch (outcome)
        {
            case PersistResultOutcome.Failed:
                throw new ConcurrentModificationException(functionId);
            case PersistResultOutcome.Success:
                InvocationHelper<TParam, TState, TReturn>.EnsureSuccess(functionId, result, allowPostponedOrSuspended);
                break;
            case PersistResultOutcome.Reschedule:
            {
                try
                {
                    await ScheduleReInvoke(functionId.InstanceId.Value, expectedEpoch);
                } catch (UnexpectedFunctionState) {} //allow this exception - the invocation has been surpassed by other execution
                InvocationHelper<TParam, TState, TReturn>.EnsureSuccess(functionId, result, allowPostponedOrSuspended);
                break;
            }
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}