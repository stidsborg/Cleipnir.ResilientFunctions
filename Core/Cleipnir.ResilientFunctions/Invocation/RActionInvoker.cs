using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Helpers.Disposables;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RActionInvoker<TParam> where TParam : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result>> _inner;
    private readonly MiddlewarePipeline _middlewarePipeline;
    private readonly IDependencyResolver? _dependencyResolver;

    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    
    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result>> inner,
        MiddlewarePipeline middlewarePipeline,
        IDependencyResolver? dependencyResolver,
        CommonInvoker commonInvoker,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _middlewarePipeline = middlewarePipeline;
        _dependencyResolver = dependencyResolver;
        _commonInvoker = commonInvoker;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, inner, disposables) = await PrepareForInvocation(functionId, param);
        if (!created) { await WaitForActionResult(functionId); return; }

        using var _ = Disposable.Combine(disposables, StartSignOfLife(functionId));
        Result result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param);
        }
        catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
        catch (Exception exception) { await PersistFailure(functionId, exception); throw; }

        await PersistResultAndEnsureSuccess(functionId, result);
    }

    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, inner, disposables) = await PrepareForInvocation(functionId, param);
        if (!created) return;

        _ = Task.Run(async () =>
        {
            using var _ = Disposable.Combine(disposables, StartSignOfLife(functionId));
            try
            {
                Result result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param);
                }
                catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
                catch (Exception exception) { await PersistFailure(functionId, exception); throw; }

                await PersistResultAndEnsureSuccess(functionId, result, allowPostponed: true);
            }
            catch (Exception exception)
            {
                _unhandledExceptionHandler.Invoke(_functionTypeId, exception);
            }
        });
    }

    public async Task ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch = null)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (inner, param, epoch, disposables) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        using var _ = Disposable.Combine(disposables, StartSignOfLife(functionId, epoch));
        Result result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param);
        }
        catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
        catch (Exception exception) { await PersistFailure(functionId, exception, epoch); throw; }

        await PersistResultAndEnsureSuccess(functionId, result, epoch);
    }

    public async Task ScheduleReInvoke(
        string instanceId, 
        IEnumerable<Status> expectedStatuses, 
        int? expectedEpoch = null,
        bool throwOnUnexpectedFunctionState = true)
    {
        try
        {
            var functionId = new FunctionId(_functionTypeId, instanceId);
            var (inner, param, epoch, disposables) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

            _ = Task.Run(async () =>
            {
                using var _ = Disposable.Combine(disposables);
                try
                {
                    Result result;
                    try
                    {
                        // *** USER FUNCTION INVOCATION *** 
                        result = await inner(param);
                    }
                    catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
                    catch (Exception exception) { await PersistFailure(functionId, exception, epoch); throw; }

                    await PersistResultAndEnsureSuccess(functionId, result, epoch, allowPostponed: true);
                }
                catch (Exception exception)
                {
                    _unhandledExceptionHandler.Invoke(_functionTypeId, exception);
                }
            });
        }
        catch (UnexpectedFunctionState) when (!throwOnUnexpectedFunctionState) {}
    }

    private async Task<PreparedInvocation> PrepareForInvocation(FunctionId functionId, TParam param)
    {
        var disposable = default(IDisposable);
        try
        {
            var scopedDependencyResolver = _dependencyResolver?.CreateScope();
            disposable = scopedDependencyResolver;
            var wrappedInner = _middlewarePipeline.WrapPipelineAroundInnerAction(
                functionId,
                InvocationMode.Direct,
                _inner,
                scopedDependencyResolver
            );
            var (persisted, runningFunction) =
                await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

            if (!persisted)
            {
                scopedDependencyResolver?.Dispose();
                scopedDependencyResolver = null;
            }

            return new PreparedInvocation(
                persisted,
                wrappedInner,
                Disposable.Combine(scopedDependencyResolver ?? Disposable.NoOp(), runningFunction)
            );
        }
        catch (Exception)
        {
            disposable?.Dispose();
            throw;
        }
    }
    private record PreparedInvocation(bool Persisted, Func<TParam, Task<Result>> Inner, IDisposable Disposables);
    
    private async Task WaitForActionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionCompletion(functionId);

    private async Task<PreparedReInvocation> PrepareForReInvocation(
        FunctionId functionId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch
    )
    {
        var scopedDependencyResolver = _dependencyResolver?.CreateScope();
        var wrappedInner = _middlewarePipeline.WrapPipelineAroundInnerAction(
            functionId,
            InvocationMode.Direct,
            _inner,
            scopedDependencyResolver
        );
        
        var (param, epoch, runningFunction) = 
            await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses, expectedEpoch);

        return new PreparedReInvocation(
            wrappedInner,
            param,
            epoch,
            Disposable.Combine(scopedDependencyResolver ?? Disposable.NoOp(), runningFunction)
        );
    }
    private record PreparedReInvocation(Func<TParam, Task<Result>> Inner, TParam Param, int Epoch, IDisposable Disposables);

    private async Task PersistFailure(FunctionId functionId, Exception exception, int expectedEpoch = 0)
        => await _commonInvoker.PersistFailure(functionId, exception, scrapbook: null, expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FunctionId functionId, Result result, int expectedEpoch = 0, bool allowPostponed = false)
    {
        await _commonInvoker.PersistResult(functionId, result, scrapbook: null, expectedEpoch);
        if (result.Outcome == Outcome.Postpone)
            _ = SleepAndThenReInvoke(functionId, postponeUntil: result.Postpone!.DateTime, expectedEpoch);
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

    private IDisposable StartSignOfLife(FunctionId functionId, int expectedEpoch = 0)
        => _commonInvoker.StartSignOfLife(functionId, expectedEpoch);
}

public class RActionInvoker<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result>> _inner;
    
    private readonly MiddlewarePipeline _middlewarePipeline;
    private readonly IDependencyResolver? _dependencyResolver;
    
    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly Type? _concreteScrapbookType;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result>> inner,
        Type? concreteScrapbookType,
        MiddlewarePipeline middlewarePipeline, 
        IDependencyResolver? dependencyResolver,
        CommonInvoker commonInvoker,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _concreteScrapbookType = concreteScrapbookType;
        _commonInvoker = commonInvoker;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _middlewarePipeline = middlewarePipeline;
        _dependencyResolver = dependencyResolver;
    }

    public async Task Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, inner, scrapbook, disposables) = await PrepareForInvocation(functionId, param);
        if (!created) { await WaitForActionCompletion(functionId); return; }

        using var _ = Disposable.Combine(disposables, StartSignOfLife(functionId));
        Result result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, scrapbook);
        }
        catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
        catch (Exception exception) { await PersistFailure(functionId, exception, scrapbook); throw; }

        await PersistResultAndEnsureSuccess(functionId, result, scrapbook);
    }

    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, inner, scrapbook, disposables) = await PrepareForInvocation(functionId, param);
        if (!created) return;

        _ = Task.Run(async () =>
        {
            using var _ = Disposable.Combine(disposables, StartSignOfLife(functionId));
            try
            {
                Result result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await inner(param, scrapbook);
                }
                catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
                catch (Exception exception) { await PersistFailure(functionId, exception, scrapbook); throw; }

                await PersistResultAndEnsureSuccess(functionId, result, scrapbook, allowPostponed: true);
            }
            catch (Exception exception)
            {
                _unhandledExceptionHandler.Invoke(_functionTypeId, exception);
            }
        });
    }

    public async Task ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch = null, Action<TScrapbook>? scrapbookUpdater = null)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (inner, param, scrapbook, epoch, disposables) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch, scrapbookUpdater);

        using var _ = Disposable.Combine(disposables, StartSignOfLife(functionId, epoch));
        Result result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await inner(param, scrapbook);
        }
        catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
        catch (Exception exception) { await PersistFailure(functionId, exception, scrapbook, epoch); throw; }

        await PersistResultAndEnsureSuccess(functionId, result, scrapbook, epoch);
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
            var (inner, param, scrapbook, epoch, disposables) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch, scrapbookUpdater);

            _ = Task.Run(async () =>
            {
                using var _ = Disposable.Combine(disposables, StartSignOfLife(functionId, epoch));
                try
                {
                    Result result;
                    try
                    {
                        // *** USER FUNCTION INVOCATION *** 
                        result = await inner(param, scrapbook);
                    }
                    catch (PostponeInvocationException exception) { result = Postpone.Until(exception.PostponeUntil); }
                    catch (Exception exception) { await PersistFailure(functionId, exception, scrapbook, epoch); throw; }

                    await PersistResultAndEnsureSuccess(functionId, result, scrapbook, epoch, allowPostponed: true);
                }
                catch (Exception exception)
                {
                    _unhandledExceptionHandler.Invoke(_functionTypeId, exception);
                }
            });
        } catch (UnexpectedFunctionState) when (!throwOnUnexpectedFunctionState) {}
    }

    private async Task WaitForActionCompletion(FunctionId functionId)
        => await _commonInvoker.WaitForActionCompletion(functionId);

    private async Task<PreparedInvocation> PrepareForInvocation(FunctionId functionId, TParam param)
    {
        var disposable = default(IDisposable);
        try
        {
            var scopedDependencyResolver = _dependencyResolver?.CreateScope();
            var wrappedInner = _middlewarePipeline.WrapPipelineAroundInnerAction(
                functionId,
                InvocationMode.Direct,
                _inner,
                scopedDependencyResolver
            );
            var scrapbook = _commonInvoker.CreateScrapbook<TScrapbook>(functionId, expectedEpoch: 0, _concreteScrapbookType);
            var (persisted, runningFunction) =
                await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: scrapbook.GetType());

            if (!persisted)
            {
                scopedDependencyResolver?.Dispose();
                scopedDependencyResolver = null;
            }
            
            return new PreparedInvocation(
                persisted,
                wrappedInner,
                scrapbook,
                Disposable.Combine(scopedDependencyResolver ?? Disposable.NoOp(), runningFunction)
            );
        }
        catch (Exception)
        {
            disposable?.Dispose();
            throw;
        }

    }
    private record PreparedInvocation(bool Persisted, Func<TParam, TScrapbook, Task<Result>> Inner, TScrapbook Scrapbook, IDisposable Disposables);
    
    private async Task<PreparedReInvocation> PrepareForReInvocation(
        FunctionId functionId, IEnumerable<Status> expectedStatuses, int? expectedEpoch,
        Action<TScrapbook>? scrapbookUpdater
    )
    {
        var scopedDependencyResolver = _dependencyResolver?.CreateScope();
        var wrappedInner = _middlewarePipeline.WrapPipelineAroundInnerAction(
            functionId,
            InvocationMode.Direct,
            _inner,
            scopedDependencyResolver
        );
        var (param, epoch, scrapbook, runningFunction) = await 
            _commonInvoker.PrepareForReInvocation<TParam, TScrapbook>(
                functionId, 
                expectedStatuses, expectedEpoch ?? 0,
                scrapbookUpdater
            );

        return new PreparedReInvocation(wrappedInner, param, scrapbook!, epoch, runningFunction);
    }
    private record PreparedReInvocation(Func<TParam, TScrapbook, Task<Result>> Inner, TParam Param, TScrapbook Scrapbook, int Epoch, IDisposable Disposables);

    private async Task PersistFailure(FunctionId functionId, Exception exception, RScrapbook scrapbook, int expectedEpoch = 0)
        => await _commonInvoker.PersistFailure(functionId, exception, scrapbook, expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FunctionId functionId, Result result, RScrapbook scrapbook, int expectedEpoch = 0, bool allowPostponed = false)
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
        
        while (DateTime.UtcNow < postponeUntil) //clock resolution means that we might wake up early 
            await Task.Yield();
        
        _ = ScheduleReInvoke(
            functionId.InstanceId.ToString(),
            expectedStatuses: new[] {Status.Postponed},
            expectedEpoch
        );
    }
    
    private IDisposable StartSignOfLife(FunctionId functionId, int expectedEpoch = 0)
        => _commonInvoker.StartSignOfLife(functionId, expectedEpoch);
}