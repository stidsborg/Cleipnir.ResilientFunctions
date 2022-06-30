using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Helpers.Disposables;

namespace Cleipnir.ResilientFunctions.Invocation;

public class FuncInvoker<TParam, TReturn> where TParam : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly System.Func<TParam, Task<Result<TReturn>>> _inner;

    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    internal FuncInvoker(
        FunctionTypeId functionTypeId,
        System.Func<TParam, Task<Result<TReturn>>> inner,
        CommonInvoker commonInvoker,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _commonInvoker = commonInvoker;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task<TReturn> Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, runningFunction) = await PersistNewFunctionInStore(functionId, param);
        if (!created) { runningFunction.Dispose(); return await WaitForFunctionResult(functionId);}

        using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId));
        Result<TReturn> result;
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
        return result.SucceedWithValue!;
    }

    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);

        var (created, runningFunction) = await PersistNewFunctionInStore(functionId, param);
        if (!created) { runningFunction.Dispose(); return; }

        _ = Task.Run(async () =>
        {
            using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId));
            try
            {
                Result<TReturn> result;
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

    public async Task<TReturn> ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, epoch, runningFunction) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);
        using var __ = Disposable.Combine(runningFunction, StartSignOfLife(functionId, epoch));

        Result<TReturn> result;
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
        return result.SucceedWithValue!;
    }

    public async Task ScheduleReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch, bool throwOnUnexpectedFunctionState = true)
    {
        try
        {
            var functionId = new FunctionId(_functionTypeId, instanceId);
            var (param, epoch, runningFunction) =
                await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);
            _ = Task.Run(async () =>
            {
                using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId, epoch));
                try
                {
                    Result<TReturn> result;
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
        } catch (UnexpectedFunctionState) when (!throwOnUnexpectedFunctionState) {}
    }

    private async Task<Tuple<bool, IDisposable>> PersistNewFunctionInStore(FunctionId functionId, TParam param) 
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<CommonInvoker.PreparedReInvocation<TParam>> PrepareForReInvocation(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch
    ) => await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses, expectedEpoch);

    private async Task PersistFailure(FunctionId functionId, Exception exception, int expectedEpoch = 0)
     => await _commonInvoker.PersistFailure(functionId, exception, scrapbook: null, expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FunctionId functionId, Result<TReturn> result, int expectedEpoch = 0, bool allowPostponed = false)
    {
        await _commonInvoker.PersistResult(functionId, result, scrapbook: null, expectedEpoch);
        if (result.Status == Status.Postponed)
            _ = SleepAndThenReInvoke(functionId, result.Postpone!.DateTime, expectedEpoch);
        CommonInvoker.EnsureSuccess(functionId, result, allowPostponed);
    } 

    private async Task SleepAndThenReInvoke(FunctionId functionId, DateTime postponeUntil, int expectedEpoch)
    {
        var delay = TimeSpanHelper.Max(postponeUntil - DateTime.UtcNow, TimeSpan.Zero);
        await Task.Delay(delay);
        await ReInvoke(
            functionId.InstanceId.ToString(),
            expectedStatuses: new[] {Status.Postponed},
            expectedEpoch
        );
    }

    private IDisposable RegisterRunningFunction() 
        => _commonInvoker.RegisterRunningFunction();

    private IDisposable StartSignOfLife(FunctionId functionId, int expectedEpoch = 0)
        => _commonInvoker.StartSignOfLife(functionId, expectedEpoch);
}

public class RFuncInvoker<TParam, TScrapbook, TReturn> 
    where TParam : notnull 
    where TScrapbook : Scrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, TScrapbook, Task<Result<TReturn>>> _inner;

    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, TScrapbook, Task<Result<TReturn>>> inner,
        CommonInvoker commonInvoker,
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _commonInvoker = commonInvoker;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task<TReturn> Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (created, runningFunction) = await PersistNewFunctionInStore(functionId, param, typeof(TScrapbook));
        if (!created) return await WaitForFunctionResult(functionId);

        using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId));
        var scrapbook = CreateScrapbook(functionId);

        Result<TReturn> result;
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
        return result.SucceedWithValue!;
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
                Result<TReturn> result;
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

    public async Task<TReturn> ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch = null, Action<TScrapbook>? scrapbookUpdater = null)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        if (scrapbookUpdater != null)
            await UpdateScrapbook(functionId, scrapbookUpdater, expectedStatuses, expectedEpoch);
        var (param, epoch, scrapbook, runningFunction) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId, epoch));

        Result<TReturn> result;
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
            if (scrapbookUpdater != null)
                await UpdateScrapbook(functionId, scrapbookUpdater, expectedStatuses, expectedEpoch);
            var (param, epoch, scrapbook, runningFunction) =
                await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

            _ = Task.Run(async () =>
            {
                using var _ = Disposable.Combine(runningFunction, StartSignOfLife(functionId, epoch));
                try
                {
                    Result<TReturn> result;
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
        } catch (UnexpectedFunctionState) when (!throwOnUnexpectedFunctionState) {}
    }

    private TScrapbook CreateScrapbook(FunctionId functionId, int epoch = 0)
        => _commonInvoker.CreateScrapbook<TScrapbook>(functionId, epoch);

    private Task UpdateScrapbook(FunctionId functionId, Action<TScrapbook> updater, IEnumerable<Status> expectedStatuses, int? expectedEpoch) 
        => _commonInvoker.UpdateScrapbook(functionId, updater, expectedStatuses, expectedEpoch);

    private async Task<Tuple<bool, IDisposable>> PersistNewFunctionInStore(FunctionId functionId, TParam param, Type scrapbookType)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType);

    private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<Tuple<TParam, int, TScrapbook, IDisposable>> PrepareForReInvocation(
        FunctionId functionId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch
    )
    {
        var (param, epoch, rScrapbook, runningFunction) = await _commonInvoker
            .PrepareForReInvocation<TParam, TScrapbook>(
                functionId,
                expectedStatuses,
                expectedEpoch
            );

        return Tuple.Create(param, epoch, rScrapbook!, runningFunction);
    }

    private async Task PersistFailure(FunctionId functionId, Exception exception, Scrapbook scrapbook, int expectedEpoch = 0)
        => await _commonInvoker.PersistFailure(functionId, exception, scrapbook, expectedEpoch);

    private async Task PersistResultAndEnsureSuccess(FunctionId functionId, Result<TReturn> result, Scrapbook scrapbook, int expectedEpoch = 0, bool allowPostponed = false)
    {
        await _commonInvoker.PersistResult(functionId, result, scrapbook, expectedEpoch);
        if (result.Status == Status.Postponed)
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

    private IDisposable StartSignOfLife(FunctionId functionId, int expectedEpoch = 0)
        => _commonInvoker.StartSignOfLife(functionId, expectedEpoch);
}