using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;

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
        var created = await PersistNewFunctionInStore(functionId, param);
        if (!created) { await WaitForActionResult(functionId); return; }

        using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId);
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
        var created = await PersistNewFunctionInStore(functionId, param);
        if (!created) return;

        _ = Task.Run(async () =>
        {
            using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId);
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
        var (param, epoch) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId, epoch);
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
        var (param, epoch) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        _ = Task.Run(async () =>
        {
            using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId, epoch);
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

    private async Task<bool> PersistNewFunctionInStore(FunctionId functionId, TParam param)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task WaitForActionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionCompletion(functionId);

    private async Task<Tuple<TParam, int>> PrepareForReInvocation(
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
        var delay = TimeSpanHelper.Max(postponeUntil - DateTime.UtcNow - TimeSpan.FromMilliseconds(100), TimeSpan.Zero);
        await Task.Delay(delay);
        _ = ScheduleReInvoke(
            functionId.InstanceId.ToString(),
            expectedStatuses: new[] {Status.Postponed},
            expectedEpoch
        );
    }

    private IDisposable CreateSignOfLifeAndRegisterRunningFunction(FunctionId functionId, int expectedEpoch = 0)
        => _commonInvoker.CreateSignOfLifeAndRegisterRunningFunction(functionId, expectedEpoch);
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
        var created = await PersistNewFunctionInStore(functionId, param, typeof(TScrapbook));
        if (!created) { await WaitForActionCompletion(functionId); return; }

        using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId);
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
        var created = await PersistNewFunctionInStore(functionId, param, typeof(TScrapbook));
        if (!created) return;

        _ = Task.Run(async () =>
        {
            using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId);
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

    public async Task ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch = null)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, scrapbook, epoch) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId, epoch);
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

    public async Task ScheduleReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch = null)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, scrapbook, epoch) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        _ = Task.Run(async () =>
        {
            using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId, epoch);
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

    private async Task<bool> PersistNewFunctionInStore(FunctionId functionId, TParam param, Type scrapbookType)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType);

    private async Task WaitForActionCompletion(FunctionId functionId)
        => await _commonInvoker.WaitForActionCompletion(functionId);

    private async Task<Tuple<TParam, TScrapbook, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses, int? expectedEpoch)
        => await _commonInvoker.PrepareForReInvocation<TParam, TScrapbook>(functionId, expectedStatuses, expectedEpoch ?? 0);

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
        var delay = TimeSpanHelper.Max(postponeUntil - DateTime.UtcNow - TimeSpan.FromMilliseconds(100), TimeSpan.Zero);
        await Task.Delay(delay);
        _ = ScheduleReInvoke(
            functionId.InstanceId.ToString(),
            expectedStatuses: new[] {Status.Postponed},
            expectedEpoch
        );
    }
    
    private IDisposable CreateSignOfLifeAndRegisterRunningFunction(FunctionId functionId, int expectedEpoch = 0)
        => _commonInvoker.CreateSignOfLifeAndRegisterRunningFunction(functionId, expectedEpoch);
}

