using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RFuncInvoker<TParam, TReturn> where TParam : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, Task<Result<TReturn>>> _inner;

    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, Task<Result<TReturn>>> inner,
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
        var created = await PersistNewFunctionInStore(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);

        using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId);
        while (true)
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

            if (await PersistResultAndEnsureSuccess(functionId, result) == InProcessWait.DoNotRetryInvocation)
                return result.SucceedWithValue!;
        }
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
                while (true)
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

                    if (await PersistResultAndEnsureSuccess(functionId, result) == InProcessWait.DoNotRetryInvocation)
                        return;
                }
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
        var (param, epoch) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId, epoch);
        while (true)
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

            if (await PersistResultAndEnsureSuccess(functionId, result, epoch) == InProcessWait.DoNotRetryInvocation)
                return result.SucceedWithValue!;
        }
    }
    
    public async Task ScheduleReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, epoch) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);

        _ = Task.Run(async () =>
        {
            using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId, epoch);
            try
            {
                while (true)
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

                    if (await PersistResultAndEnsureSuccess(functionId, result, epoch) == InProcessWait.DoNotRetryInvocation)
                        return;
                }
            }
            catch (Exception exception)
            {
                _unhandledExceptionHandler.Invoke(_functionTypeId, exception);
            }
        });
    }

    private async Task<bool> PersistNewFunctionInStore(FunctionId functionId, TParam param)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<Tuple<TParam, int>> PrepareForReInvocation(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch
    ) => await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses, expectedEpoch);

    private async Task PersistFailure(FunctionId functionId, Exception exception, int expectedEpoch = 0)
     => await _commonInvoker.PersistFailure(functionId, exception, scrapbook: null, expectedEpoch);

    private async Task<InProcessWait> PersistResultAndEnsureSuccess(FunctionId functionId, Result<TReturn> result, int expectedEpoch = 0)
        => await _commonInvoker.PersistResultAndEnsureSuccess(functionId, result, scrapbook: null, expectedEpoch);

    private IDisposable CreateSignOfLifeAndRegisterRunningFunction(FunctionId functionId, int expectedEpoch = 0)
        => _commonInvoker.CreateSignOfLifeAndRegisterRunningFunction(functionId, expectedEpoch);
}

public class RFuncInvoker<TParam, TScrapbook, TReturn> 
    where TParam : notnull 
    where TScrapbook : RScrapbook, new()
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
        var created = await PersistNewFunctionInStore(functionId, param, typeof(TScrapbook));
        if (!created) return await WaitForFunctionResult(functionId);

        using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId);
        var scrapbook = CreateScrapbook(functionId);
        while (true)
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

            if (await PersistResultAndEnsureSuccess(functionId, result, scrapbook) == InProcessWait.DoNotRetryInvocation)
                return result.SucceedWithValue!;
        }
    }

    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var created = await PersistNewFunctionInStore(functionId, param, typeof(TScrapbook));
        if (!created) return;
        var metadata = new Metadata<TParam>(functionId, param);

        _ = Task.Run(async () =>
        {
            using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId);
            var scrapbook = CreateScrapbook(functionId);
            try
            {
                while (true)
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

                    if (await PersistResultAndEnsureSuccess(functionId, result, scrapbook) == InProcessWait.DoNotRetryInvocation)
                        return;
                }
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
        var (param, scrapbook, epoch) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);
        var metadata = new Metadata<TParam>(functionId, param);

        using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId, epoch);
        while (true)
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

            if (await PersistResultAndEnsureSuccess(functionId, result, scrapbook, epoch) == InProcessWait.DoNotRetryInvocation)
                return result.SucceedWithValue!;
        }
    }
    
    public async Task ScheduleReInvoke(string instanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, scrapbook, epoch) = await PrepareForReInvocation(functionId, expectedStatuses, expectedEpoch);
        var metadata = new Metadata<TParam>(functionId, param);
            
        _ = Task.Run(async () =>
        {
            using var _ = CreateSignOfLifeAndRegisterRunningFunction(functionId, epoch);
            try
            {
                while (true)
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

                    if (await PersistResultAndEnsureSuccess(functionId, result, scrapbook, epoch) == InProcessWait.DoNotRetryInvocation)
                        return;
                }
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

    private async Task<TReturn> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<Tuple<TParam, TScrapbook, int>> PrepareForReInvocation(
        FunctionId functionId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch
    ) => await _commonInvoker
        .PrepareForReInvocation<TParam, TScrapbook>(
            functionId,
            expectedStatuses,
            expectedEpoch
        );

    private async Task PersistFailure(FunctionId functionId, Exception exception, RScrapbook scrapbook, int expectedEpoch = 0)
        => await _commonInvoker.PersistFailure(functionId, exception, scrapbook, expectedEpoch);

    private async Task<InProcessWait> PersistResultAndEnsureSuccess(FunctionId functionId, Result<TReturn> result, RScrapbook scrapbook, int expectedEpoch = 0)
        => await _commonInvoker.PersistResultAndEnsureSuccess(functionId, result, scrapbook, expectedEpoch);

    private IDisposable CreateSignOfLifeAndRegisterRunningFunction(FunctionId functionId, int expectedEpoch = 0)
        => _commonInvoker.CreateSignOfLifeAndRegisterRunningFunction(functionId, expectedEpoch);
}