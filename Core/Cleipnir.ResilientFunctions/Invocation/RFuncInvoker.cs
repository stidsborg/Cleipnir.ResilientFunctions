using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Helpers.Disposables;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RFuncInvoker<TEntity, TParam, TScrapbook, TReturn> 
    where TParam : notnull 
    where TScrapbook : RScrapbook, new()
    where TEntity : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TEntity,Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>>? _innerMethodSelector;
    private readonly Func<TParam,TScrapbook,Context,Task<Result<TReturn>>>? _inner;
    private readonly IDependencyResolver? _dependencyResolver;
    private readonly MiddlewarePipeline _middlewarePipeline;
    
    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly Type? _concreteScrapbookType;

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>? inner,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>>? innerMethodSelector,
        IDependencyResolver? dependencyResolver,
        MiddlewarePipeline middlewarePipeline,
        Type? concreteScrapbookType,
        CommonInvoker commonInvoker,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _innerMethodSelector = innerMethodSelector;
        _dependencyResolver = dependencyResolver;
        _middlewarePipeline = middlewarePipeline;
        _concreteScrapbookType = concreteScrapbookType;
        _commonInvoker = commonInvoker;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task<TReturn> Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, inner, scrapbook, context, disposables) = await PrepareForInvocation(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);
        using var _ = disposables;

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, scrapbook, context);
        }
        catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
        catch (Exception exception) { await PersistFailure(functionId, exception, scrapbook, context); throw; }

        await PersistResultAndEnsureSuccess(functionId, result, scrapbook, context);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, inner, scrapbook, context, disposables) = await PrepareForInvocation(functionId, param);
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
                catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
                catch (Exception exception) { await PersistFailure(functionId, exception, scrapbook, context); throw; }

                await PersistResultAndEnsureSuccess(functionId, result, scrapbook, context, allowPostponed: true);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
            finally{ disposables.Dispose(); }
        });
    }

    public async Task<TReturn> ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch = null, Action<TScrapbook>? scrapbookUpdater = null)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (inner, param, scrapbook, context, epoch, disposables) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch, scrapbookUpdater);
        using var _ = disposables;

        Result<TReturn> result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, scrapbook, context);
        }
        catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
        catch (Exception exception) { await PersistFailure(functionId, exception, scrapbook, context, epoch); throw; }

        await PersistResultAndEnsureSuccess(functionId, result, scrapbook, context, epoch);
        return result.SucceedWithValue!;
    }

    public async Task ScheduleReInvoke(
        string instanceId, 
        IEnumerable<Status> expectedStatuses, 
        int? expectedEpoch = null, 
        Action<TScrapbook>? scrapbookUpdater = null,
        bool throwOnUnexpectedFunctionState = true)
    {
        try
        {
            var functionId = new FunctionId(_functionTypeId, instanceId);
            var (inner, param, scrapbook, context, epoch, disposables) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch, scrapbookUpdater);

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
                    catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
                    catch (Exception exception) { await PersistFailure(functionId, exception, scrapbook, context, epoch); throw; }

                    await PersistResultAndEnsureSuccess(functionId, result, scrapbook, context, epoch, allowPostponed: true);
                }
                catch (Exception exception) { _unhandledExceptionHandler.Invoke(_functionTypeId, exception); }
                finally{ disposables.Dispose(); }
            });
        } catch (UnexpectedFunctionState) when (!throwOnUnexpectedFunctionState) {}
    }
    
    private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<PreparedInvocation> PrepareForInvocation(FunctionId functionId, TParam param)
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
            
            
            var wrappedInner = _middlewarePipeline.WrapPipelineAroundInnerFunc(inner, scopedDependencyResolver);
            var scrapbook = _commonInvoker.CreateScrapbook<TScrapbook>(functionId, expectedEpoch: 0, _concreteScrapbookType);
            var (persisted, runningFunction) = await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbook);
            disposables.Add(runningFunction);
            disposables.Add(_commonInvoker.StartSignOfLife(functionId, epoch: 0));
            
            success = persisted;
            return new PreparedInvocation(
                persisted,
                wrappedInner,
                scrapbook,
                new Context(functionId, InvocationMode.Direct),
                Disposable.Combine(disposables)
            );
        }
        finally
        {
            if (!success) Disposable.Combine(disposables).Dispose();
        }
    }
    private record PreparedInvocation(bool Persisted, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>> Inner, TScrapbook Scrapbook, Context Context, IDisposable Disposables);

    private async Task<PreparedReInvocation> PrepareForReInvocation(
        FunctionId functionId, IEnumerable<Status> expectedStatuses, int? expectedEpoch,
        Action<TScrapbook>? scrapbookUpdater
    )
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
           
            var wrappedInner = _middlewarePipeline.WrapPipelineAroundInnerFunc(inner, scopedDependencyResolver);

            var (param, epoch, scrapbook, runningFunction) = await 
                _commonInvoker.PrepareForReInvocation<TParam, TScrapbook>(
                    functionId, 
                    expectedStatuses, expectedEpoch ?? 0,
                    scrapbookUpdater
                );
            disposables.Add(runningFunction);
            disposables.Add(_commonInvoker.StartSignOfLife(functionId, epoch));

            return new PreparedReInvocation(
                wrappedInner,
                param,
                scrapbook!,
                new Context(functionId, InvocationMode.Retry),
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

    private async Task PersistFailure(FunctionId functionId, Exception exception, TScrapbook scrapbook, Context context, int expectedEpoch = 0)
        => await _commonInvoker.PersistFailure(functionId, exception, scrapbook, expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FunctionId functionId, Result<TReturn> result, TScrapbook scrapbook, Context context, int expectedEpoch = 0, bool allowPostponed = false)
    {
        await _commonInvoker.PersistResult(functionId, result, scrapbook, expectedEpoch);
        if (result.Outcome == Outcome.Postpone)
            _ = SleepAndThenReInvoke(functionId, result.Postpone!.DateTime, expectedEpoch);
        CommonInvoker.EnsureSuccess(functionId, result, allowPostponed);
    }
    
    private async Task SleepAndThenReInvoke(FunctionId functionId, DateTime postponeUntil, int expectedEpoch)
    {
        var delay = TimeSpanHelper.Max(postponeUntil - DateTime.UtcNow, TimeSpan.Zero);
        await Task.Delay(delay);
        _ = ScheduleReInvoke(
            functionId.InstanceId.ToString(),
            expectedStatuses: new[] {Status.Postponed},
            expectedEpoch
        );
    }
}