using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Helpers.Disposables;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RActionInvoker<TParam> where TParam : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result>> _inner;

    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    
    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result>> inner,
        CommonInvoker commonInvoker,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _commonInvoker = commonInvoker;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, runningFunction) = await PersistNewFunctionInStore(functionId, param);
        if (!created) { await WaitForActionResult(functionId); return; }

        using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId));
        Result result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await _inner(param);
        }
        catch (Exception exception)
        {
            await PersistFailure(functionId, exception);
            throw;
        }

        await PersistResultAndEnsureSuccess(functionId, result);
    }

    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, runningFunction) = await PersistNewFunctionInStore(functionId, param);
        if (!created) return;

        _ = Task.Run(async () =>
        {
            using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId));
            try
            {
                Result result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _inner(param);
                }
                catch (Exception exception)
                {
                    await PersistFailure(functionId, exception);
                    throw;
                }

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
        var (param, epoch, runningFunction) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId, epoch));
        Result result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await _inner(param);
        }
        catch (Exception exception)
        {
            await PersistFailure(functionId, exception, epoch);
            throw;
        }

        await PersistResultAndEnsureSuccess(functionId, result, epoch);
    }

    public async Task ScheduleReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch = null)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, epoch, runningFunction) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        _ = Task.Run(async () =>
        {
            using var _ = Disposable.Combine(runningFunction);
            try
            {
                Result result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _inner(param);
                }
                catch (Exception exception)
                {
                    await PersistFailure(functionId, exception, epoch);
                    throw;
                }

                await PersistResultAndEnsureSuccess(functionId, result, epoch, allowPostponed: true);
            }
            catch (Exception exception)
            {
                _unhandledExceptionHandler.Invoke(_functionTypeId, exception);
            }
        });
    }

    private async Task<Tuple<bool, IDisposable>> PersistNewFunctionInStore(FunctionId functionId, TParam param)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task WaitForActionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionCompletion(functionId);

    private async Task<CommonInvoker.PreparedReInvocation<TParam>> PrepareForReInvocation(
        FunctionId functionId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch)
        => await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses, expectedEpoch);

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
    
    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result>> inner,
        CommonInvoker commonInvoker,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _commonInvoker = commonInvoker;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, runningFunction) = await PersistNewFunctionInStore(functionId, param, typeof(TScrapbook));
        if (!created) { await WaitForActionCompletion(functionId); return; }

        using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId));
        var scrapbook = CreateScrapbook(functionId);
        Result result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await _inner(param, scrapbook);
        }
        catch (Exception exception)
        {
            await PersistFailure(functionId, exception, scrapbook);
            throw;
        }

        await PersistResultAndEnsureSuccess(functionId, result, scrapbook);
    }

    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, runningFunction) = await PersistNewFunctionInStore(functionId, param, typeof(TScrapbook));
        if (!created) return;

        _ = Task.Run(async () =>
        {
            using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId));
            var scrapbook = CreateScrapbook(functionId);
            try
            {
                Result result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _inner(param, scrapbook);
                }
                catch (Exception exception)
                {
                    await PersistFailure(functionId, exception, scrapbook);
                    throw;
                }

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
        if (scrapbookUpdater != null) 
            await UpdateScrapbook(functionId, scrapbookUpdater, expectedStatuses, expectedEpoch);
        var (param, epoch, scrapbook, runningFunction) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId, epoch));
        Result result;
        try
        {
            // *** USER FUNCTION INVOCATION *** 
            result = await _inner(param, scrapbook);
        }
        catch (Exception exception)
        {
            await PersistFailure(functionId, exception, scrapbook, epoch);
            throw;
        }

        await PersistResultAndEnsureSuccess(functionId, result, scrapbook, epoch);
    }

    public async Task ScheduleReInvoke(
        string instanceId, 
        IEnumerable<Status> expectedStatuses, 
        int? expectedEpoch = null, 
        Action<TScrapbook>? scrapbookUpdater = null)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        if (scrapbookUpdater != null)
            await UpdateScrapbook(functionId, scrapbookUpdater, expectedStatuses, expectedEpoch);
        var (param, epoch, scrapbook, runningFunction) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        _ = Task.Run(async () =>
        {
            using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId, epoch));
            try
            {
                Result result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _inner(param, scrapbook);
                }
                catch (Exception exception)
                {
                    await PersistFailure(functionId, exception, scrapbook, epoch);
                    throw;
                }

                await PersistResultAndEnsureSuccess(functionId, result, scrapbook, epoch, allowPostponed: true);
            }
            catch (Exception exception)
            {
                _unhandledExceptionHandler.Invoke(_functionTypeId, exception);
            }
        });
    }

    private TScrapbook CreateScrapbook(FunctionId functionId, int epoch = 0)
        => _commonInvoker.CreateScrapbook<TScrapbook>(functionId, epoch);

    private Task UpdateScrapbook(FunctionId functionId, Action<TScrapbook> updater, IEnumerable<Status> expectedStatuses, int? expectedEpoch) 
        => _commonInvoker.UpdateScrapbook(functionId, updater, expectedStatuses, expectedEpoch);

    private async Task<Tuple<bool, IDisposable>> PersistNewFunctionInStore(FunctionId functionId, TParam param, Type scrapbookType)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType);

    private async Task WaitForActionCompletion(FunctionId functionId)
        => await _commonInvoker.WaitForActionCompletion(functionId);

    private async Task<Tuple<TParam, int, TScrapbook, IDisposable>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses, int? expectedEpoch)
    {
        var (param, epoch, rScrapbook, runningFunction) = await 
            _commonInvoker.PrepareForReInvocation<TParam, TScrapbook>(functionId, expectedStatuses, expectedEpoch ?? 0);

        return Tuple.Create(param, epoch, rScrapbook!, runningFunction);
    }

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

