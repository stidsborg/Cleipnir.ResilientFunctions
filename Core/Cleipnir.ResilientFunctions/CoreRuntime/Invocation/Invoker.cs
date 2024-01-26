﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers.Disposables;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Invoker<TParam, TScrapbook, TReturn> 
    where TParam : notnull 
    where TScrapbook : RScrapbook, new() 
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam,TScrapbook,Context,Task<Result<TReturn>>> _inner;
    
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly Utilities _utilities;

    internal Invoker(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> inner,
        InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper,
        UnhandledExceptionHandler unhandledExceptionHandler,
        Utilities utilities)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _invocationHelper = invocationHelper;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _utilities = utilities;
    }

    public async Task<TReturn> Invoke(string functionInstanceId, TParam param, TScrapbook? scrapbook = null)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        (var created, scrapbook, var context, var disposables) = await PrepareForInvocation(functionId, param, scrapbook);
        if (!created) return await WaitForFunctionResult(functionId);
        using var _ = disposables;

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await _inner(param, scrapbook, context);
        }
        catch (Exception exception) { await PersistFailure(functionId, exception, param, scrapbook, sendResultTo: default); throw; }

        await PersistResultAndEnsureSuccess(functionId, result, param, scrapbook, sendResultTo: default);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleInvoke(string functionInstanceId, TParam param, TScrapbook? scrapbook, FunctionId? sendResultTo)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        (var created, scrapbook, var context, var disposables) = await PrepareForInvocation(functionId, param, scrapbook);
        if (!created) return;

        _ = Task.Run(async () =>
        {
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _inner(param, scrapbook, context);
                    await PublishResult(sendResultTo, functionId, result);
                }
                catch (Exception exception) { await PersistFailure(functionId, exception, param, scrapbook, sendResultTo); throw; }

                await PersistResultAndEnsureSuccess(functionId, result, param, scrapbook, allowPostponedOrSuspended: true, sendResultTo: sendResultTo);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
            finally{ disposables.Dispose(); }
        });
    }
    
    public async Task ScheduleAt(string instanceId, TParam param, DateTime scheduleAt, TScrapbook? scrapbook, FunctionId? sendResultTo)
    {
        if (scheduleAt.ToUniversalTime() <= DateTime.UtcNow)
        {
            await ScheduleInvoke(instanceId, param, scrapbook, sendResultTo);
            return;
        }

        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (_, disposable) = await _invocationHelper.PersistFunctionInStore(
            functionId,
            param,
            scrapbook ?? new TScrapbook(),
            scheduleAt,
            sendResultTo
        );

        disposable.Dispose();
    }

    public async Task<TReturn> ReInvoke(string instanceId, int expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (inner, param, scrapbook, context, epoch, disposables, sendResultTo) = await PrepareForReInvocation(functionId, expectedEpoch);
        using var _ = disposables;

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, scrapbook, context);
            await PublishResult(sendResultTo, functionId, result);
        }
        catch (Exception exception) { await PersistFailure(functionId, exception, param, scrapbook, sendResultTo, epoch); throw; }

        await PersistResultAndEnsureSuccess(functionId, result, param, scrapbook, sendResultTo, epoch);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleReInvoke(string instanceId, int expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (inner, param, scrapbook, context, epoch, disposables, sendResultTo) = await PrepareForReInvocation(functionId, expectedEpoch);

        _ = Task.Run(async () =>
        {
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param, scrapbook, context);
                    await PublishResult(sendResultTo, functionId, result);
                }
                catch (Exception exception) { await PersistFailure(functionId, exception, param, scrapbook, sendResultTo, epoch); throw; }

                await PersistResultAndEnsureSuccess(functionId, result, param, scrapbook, sendResultTo, epoch, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
            finally{ disposables.Dispose(); }
        });
    }
    
    private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        => await _invocationHelper.WaitForFunctionResult(functionId, allowPostponeAndSuspended: false);

    private async Task<PreparedInvocation> PrepareForInvocation(FunctionId functionId, TParam param, TScrapbook? scrapbook)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        var success = false;
        try
        {
            scrapbook ??= new TScrapbook();
            _invocationHelper.InitializeScrapbook(functionId, param, scrapbook, epoch: 0);
            
            var (persisted, runningFunction) = 
                await _invocationHelper.PersistFunctionInStore(
                    functionId, 
                    param, 
                    scrapbook,
                    scheduleAt: null,
                    sendResultTo: null
                );
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(functionId, epoch: 0));
            
            success = persisted;
            var messages = await _invocationHelper.CreateMessages(functionId, ScheduleReInvoke, sync: false);
            var activity = await _invocationHelper.CreateActivities(functionId, sync: false);
            var context = new Context(functionId, messages, activity, _utilities);
            disposables.Add(context);
            
            return new PreparedInvocation(
                persisted,
                scrapbook,
                context,
                Disposable.Combine(disposables)
            );
        }
        finally
        {
            if (!success) Disposable.Combine(disposables).Dispose();
        }
    }
    private record PreparedInvocation(bool Persisted, TScrapbook Scrapbook, Context Context, IDisposable Disposables);

    private async Task<PreparedReInvocation> PrepareForReInvocation(FunctionId functionId, int expectedEpoch)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        try
        {
            var (param, epoch, scrapbook, runningFunction, sendResultTo) = 
                await _invocationHelper.PrepareForReInvocation(functionId, expectedEpoch);
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartLeaseUpdater(functionId, epoch));
            
            var messagesTask = Task.Run(() => _invocationHelper.CreateMessages(functionId, ScheduleReInvoke, sync: true));
            var activitiesTask = Task.Run(() => _invocationHelper.CreateActivities(functionId, sync: true));
            var context = new Context(functionId, await messagesTask, await activitiesTask, _utilities);
            disposables.Add(context);

            return new PreparedReInvocation(
                _inner,
                param,
                scrapbook,
                context,
                epoch,
                Disposable.Combine(disposables),
                sendResultTo
            );
        }
        catch(Exception)
        {
            Disposable.Combine(disposables).Dispose();
            throw;
        }
    }
    private record PreparedReInvocation(Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> Inner, TParam Param, TScrapbook Scrapbook, Context Context, int Epoch, IDisposable Disposables, FunctionId? SendResultTo);

    private async Task PersistFailure(FunctionId functionId, Exception exception, TParam param, TScrapbook scrapbook, FunctionId? sendResultTo, int expectedEpoch = 0)
        => await _invocationHelper.PersistFailure(functionId, exception, param, scrapbook, sendResultTo, expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FunctionId functionId, Result<TReturn> result, TParam param, TScrapbook scrapbook, FunctionId? sendResultTo, int expectedEpoch = 0, bool allowPostponedOrSuspended = false)
    {
        if (sendResultTo != null && result.Succeed)
            await _invocationHelper.PublishFunctionCompletionResult(sendResultTo, functionId, result);
        
        var success = await _invocationHelper.PersistResult(functionId, result, param, scrapbook, sendResultTo, expectedEpoch);
        if (success)
            InvocationHelper<TParam, TScrapbook, TReturn>.EnsureSuccess(functionId, result, allowPostponedOrSuspended);
        else
            throw new ConcurrentModificationException(functionId);
    }

    public Task PublishResult<T>(FunctionId? recipient, FunctionId sender, Result<T> result)
    {
        if (recipient == null) return Task.CompletedTask; 
        
        return _invocationHelper.PublishFunctionCompletionResult(recipient, sender, result);
    }
}