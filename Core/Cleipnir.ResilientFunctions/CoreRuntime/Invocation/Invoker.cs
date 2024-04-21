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
    where TParam : notnull 
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Workflow, Task<Result<TReturn>>> _inner;
    
    private readonly InvocationHelper<TParam, TReturn> _invocationHelper;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly Utilities _utilities;
    private readonly Func<FunctionId, MessageWriter> _messageWriterFunc;

    internal Invoker(
        FunctionTypeId functionTypeId,
        Func<TParam, Workflow, Task<Result<TReturn>>> inner,
        InvocationHelper<TParam, TReturn> invocationHelper,
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

    public async Task<TReturn> Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
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

    public async Task ScheduleInvoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
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
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
        });
    }
    
    public async Task ScheduleAt(string instanceId, TParam param, DateTime scheduleAt)
    {
        if (scheduleAt.ToUniversalTime() <= DateTime.UtcNow)
        {
            await ScheduleInvoke(instanceId, param);
            return;
        }

        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (_, disposable) = await _invocationHelper.PersistFunctionInStore(
            functionId,
            param,
            scheduleAt
        );

        disposable.Dispose();
    }

    public async Task<TReturn> ReInvoke(string instanceId, int expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
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

    public async Task ScheduleReInvoke(string instanceId, int expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
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
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
        });
    }
    
    private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        => await _invocationHelper.WaitForFunctionResult(functionId, allowPostponedAndSuspended: false);

    private async Task<PreparedInvocation> PrepareForInvocation(FunctionId functionId, TParam param)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        var success = false;
        try
        {
            var (persisted, runningFunction) =
                await _invocationHelper.PersistFunctionInStore(
                    functionId,
                    param,
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
            var states = await _invocationHelper.CreateStates(functionId, defaultState: null, sync: false);
            var workflow = new Workflow(functionId, messages, effect, states, _utilities, _messageWriterFunc);

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

    private async Task<PreparedReInvocation> PrepareForReInvocation(FunctionId functionId, int expectedEpoch)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        try
        {
            var (param, epoch, defaultState, runningFunction) = 
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
            var effectsTask = Task.Run(() => _invocationHelper.CreateEffect(functionId, sync: true));
            var statesTask = Task.Run(() => _invocationHelper.CreateStates(functionId, defaultState, sync: true));
            var workflow = new Workflow(functionId, await messagesTask, await effectsTask, await statesTask, _utilities, _messageWriterFunc);

            return new PreparedReInvocation(
                _inner,
                param,
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
    private record PreparedReInvocation(Func<TParam, Workflow, Task<Result<TReturn>>> Inner, TParam Param, Workflow Workflow, int Epoch, IDisposable Disposables);

    private async Task PersistFailure(FunctionId functionId, Exception exception, TParam param, Workflow workflow, int expectedEpoch = 0)
        => await _invocationHelper.PersistFailure(functionId, exception, param, workflow.States.SerializeDefaultState(), expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FunctionId functionId, Result<TReturn> result, TParam param, Workflow workflow, int expectedEpoch = 0, bool allowPostponedOrSuspended = false)
    {
        if (result.Succeed && result.SucceedWithValue is Task)
        {
            var serializationException = new SerializationException("Unable to serialize result of Task-type");
            await _invocationHelper.PersistFailure(functionId, serializationException, param, workflow.States.SerializeDefaultState(), expectedEpoch);
            throw serializationException;
        }
            
        var outcome = await _invocationHelper.PersistResult(functionId, result, param, workflow.States.SerializeDefaultState(), expectedEpoch);
        switch (outcome)
        {
            case PersistResultOutcome.Failed:
                throw new ConcurrentModificationException(functionId);
            case PersistResultOutcome.Success:
                InvocationHelper<TParam, TReturn>.EnsureSuccess(functionId, result, allowPostponedOrSuspended);
                break;
            case PersistResultOutcome.Reschedule:
            {
                try
                {
                    await ScheduleReInvoke(functionId.InstanceId.Value, expectedEpoch);
                } catch (UnexpectedFunctionState) {} //allow this exception - the invocation has been surpassed by other execution
                InvocationHelper<TParam, TReturn>.EnsureSuccess(functionId, result, allowPostponedOrSuspended);
                break;
            }
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}