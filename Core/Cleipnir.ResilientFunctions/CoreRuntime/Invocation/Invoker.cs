using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Helpers.Disposables;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Invoker<TEntity, TParam, TScrapbook, TReturn> 
    where TParam : notnull 
    where TScrapbook : RScrapbook, new()
    where TEntity : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TEntity,Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>>? _innerMethodSelector;
    private readonly Func<TParam,TScrapbook,Context,Task<Result<TReturn>>>? _inner;
    private readonly IDependencyResolver? _dependencyResolver;
    private readonly MiddlewarePipeline _middlewarePipeline;
    
    private readonly InvocationHelper<TParam, TScrapbook, TReturn> _invocationHelper;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly Utilities _utilities;

    internal Invoker(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>? inner,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>>? innerMethodSelector,
        IDependencyResolver? dependencyResolver,
        MiddlewarePipeline middlewarePipeline,
        InvocationHelper<TParam, TScrapbook, TReturn> invocationHelper,
        UnhandledExceptionHandler unhandledExceptionHandler,
        Utilities utilities)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _innerMethodSelector = innerMethodSelector;
        _dependencyResolver = dependencyResolver;
        _middlewarePipeline = middlewarePipeline;
        _invocationHelper = invocationHelper;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _utilities = utilities;
    }

    public async Task<TReturn> Invoke(string functionInstanceId, TParam param, TScrapbook? scrapbook = null)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        (var created, var inner, scrapbook, var context, var disposables) = await PrepareForInvocation(functionId, param, scrapbook);
        if (!created) return await WaitForFunctionResult(functionId);
        using var _ = disposables;

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, scrapbook, context);
        }
        catch (Exception exception) { await PersistFailure(functionId, exception, param, scrapbook); throw; }

        await PersistResultAndEnsureSuccess(functionId, result, param, scrapbook);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleInvoke(string functionInstanceId, TParam param, TScrapbook? scrapbook)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        (var created, var inner, scrapbook, var context, var disposables) = await PrepareForInvocation(functionId, param, scrapbook);
        if (!created) return;

        _ = Task.Run(async () =>
        {
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param, scrapbook, context);
                }
                catch (Exception exception) { await PersistFailure(functionId, exception, param, scrapbook); throw; }

                await PersistResultAndEnsureSuccess(functionId, result, param, scrapbook, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
            finally{ disposables.Dispose(); }
        });
    }

    public async Task<TReturn> ReInvoke(string instanceId, int expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (inner, param, scrapbook, context, epoch, disposables) = await PrepareForReInvocation(functionId, expectedEpoch);
        using var _ = disposables;

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, scrapbook, context);
        }
        catch (Exception exception) { await PersistFailure(functionId, exception, param, scrapbook, epoch); throw; }

        await PersistResultAndEnsureSuccess(functionId, result, param, scrapbook, epoch);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleReInvoke(string instanceId, int expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (inner, param, scrapbook, context, epoch, disposables) = await PrepareForReInvocation(functionId, expectedEpoch);

        _ = Task.Run(async () =>
        {
            try
            {
                Result<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param, scrapbook, context);
                }
                catch (Exception exception) { await PersistFailure(functionId, exception, param, scrapbook, epoch); throw; }

                await PersistResultAndEnsureSuccess(functionId, result, param, scrapbook, epoch, allowPostponedOrSuspended: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
            finally{ disposables.Dispose(); }
        });
    }
    
    private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        => await _invocationHelper.WaitForFunctionResult(functionId);

    private async Task<PreparedInvocation> PrepareForInvocation(FunctionId functionId, TParam param, TScrapbook? scrapbook)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        var success = false;
        try
        {
            Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> inner;
            IScopedDependencyResolver? scopedDependencyResolver = null;
            if (_inner != null)
                inner = _inner;
            else
            {
                scopedDependencyResolver = _dependencyResolver!.CreateScope();
                disposables.Add(scopedDependencyResolver);
                var entity = scopedDependencyResolver.Resolve<TEntity>();
                inner = _innerMethodSelector!(entity);
            }

            scrapbook ??= new TScrapbook();
            _invocationHelper.InitializeScrapbook(functionId, param, scrapbook, epoch: 0);
            
            var wrappedInner = _middlewarePipeline.WrapPipelineAroundInner(
                inner,
                scopedDependencyResolver,
                new PreCreationParameters<TParam>(param, scrapbook.StateDictionary, functionId)
            );
            
            var (persisted, runningFunction) = await _invocationHelper.PersistFunctionInStore(functionId, param, scrapbook);
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartSignOfLife(functionId, epoch: 0));
            
            success = persisted;
            return new PreparedInvocation(
                persisted,
                wrappedInner,
                scrapbook,
                new Context(functionId, InvocationMode.Direct, _invocationHelper.CreateAndInitializeEventSource(functionId, ScheduleReInvoke), _utilities),
                Disposable.Combine(disposables)
            );
        }
        finally
        {
            if (!success) Disposable.Combine(disposables).Dispose();
        }
    }
    private record PreparedInvocation(bool Persisted, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> Inner, TScrapbook Scrapbook, Context Context, IDisposable Disposables);

    private async Task<PreparedReInvocation> PrepareForReInvocation(FunctionId functionId, int expectedEpoch)
    {
        var disposables = new List<IDisposable>(capacity: 3);
        try
        {
            Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> inner;
            var scopedDependencyResolver = _dependencyResolver?.CreateScope();
            if (_inner != null)
                inner = _inner;
            else
            {
                disposables.Add(scopedDependencyResolver!);
                var entity = scopedDependencyResolver!.Resolve<TEntity>();
                inner = _innerMethodSelector!(entity);
            }

            var wrappedInner = _middlewarePipeline.WrapPipelineAroundInner(
                inner,
                scopedDependencyResolver,
                preCreationParameters: null
            );

            var (param, epoch, scrapbook, runningFunction) = 
                await _invocationHelper.PrepareForReInvocation(functionId, expectedEpoch);
            disposables.Add(runningFunction);
            disposables.Add(_invocationHelper.StartSignOfLife(functionId, epoch));

            return new PreparedReInvocation(
                wrappedInner,
                param,
                scrapbook,
                new Context(functionId, InvocationMode.Retry, _invocationHelper.CreateAndInitializeEventSource(functionId, ScheduleReInvoke), _utilities),
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
    private record PreparedReInvocation(Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> Inner, TParam Param, TScrapbook Scrapbook, Context Context, int Epoch, IDisposable Disposables);

    private async Task PersistFailure(FunctionId functionId, Exception exception, TParam param, TScrapbook scrapbook, int expectedEpoch = 0)
        => await _invocationHelper.PersistFailure(functionId, exception, param, scrapbook, expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FunctionId functionId, Result<TReturn> result, TParam param, TScrapbook scrapbook, int expectedEpoch = 0, bool allowPostponedOrSuspended = false)
    {
        await _invocationHelper.PersistResult(functionId, result, param, scrapbook, expectedEpoch);
        if (result.Outcome == Outcome.Postpone)
            _ = SleepAndThenReInvoke(functionId, result.Postpone!.DateTime, expectedEpoch);
        InvocationHelper<TParam, TScrapbook, TReturn>.EnsureSuccess(functionId, result, allowPostponedOrSuspended);
    }
    
    private async Task SleepAndThenReInvoke(FunctionId functionId, DateTime postponeUntil, int expectedEpoch)
    {
        var delay = TimeSpanHelper.Max(postponeUntil - DateTime.UtcNow, TimeSpan.Zero);
        await Task.Delay(delay);
        using var suppressedFlow = ExecutionContext.SuppressFlow();
        _ = ScheduleReInvoke(functionId.InstanceId.ToString(), expectedEpoch);
    }
}