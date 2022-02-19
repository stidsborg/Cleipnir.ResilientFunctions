using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RActionInvoker<TParam> where TParam : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, Task<RResult>> _func;

    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, Task<RResult>> func,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
        ShutdownCoordinator shutdownCoordinator, 
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task<RResult> Invoke(TParam param)
    {
        var functionId = CreateFunctionId(param);
        var created = await PersistFunctionInStore(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            RResult result;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                result = await _func(param);
            }
            catch (Exception exception)
            {
                await ProcessUnhandledException(functionId, exception);
                return new Fail(exception);
            }

            await ProcessResult(functionId, result);
            return result;
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    public async Task ScheduleInvocation(TParam param)
    {
        var functionId = CreateFunctionId(param);
        var created = await PersistFunctionInStore(functionId, param);
        if (!created) return;
        
        _ = Task.Run(async () =>
        {
            try
            {
                using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
                _shutdownCoordinator.RegisterRunningRFunc();
                RResult result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _func(param);
                }
                catch (Exception exception)
                {
                    await ProcessUnhandledException(functionId, exception);
                    return;
                }

                await ProcessResult(functionId, result);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(exception); }
            finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
        });
    }
    
    public async Task<RResult> ReInvoke(string functionInstanceId, IEnumerable<Status> expectedStatuses)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (param, epoch) = await PrepareForReInvocation(functionId, expectedStatuses);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            RResult result;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                result = await _func(param);
            }
            catch (Exception exception)
            {
                await ProcessUnhandledException(functionId, exception, epoch);
                return new Fail(exception);
            }

            await ProcessResult(functionId, result, epoch);
            return result;
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param) 
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task<RResult> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionResult(functionId);

    private async Task<Tuple<TParam, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        => await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses);

    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, int epoch = 0)
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook: null, epoch);

    private Task ProcessResult(FunctionId functionId, RResult result, int epoch = 0)
        => _commonInvoker.ProcessResult(functionId, result, scrapbook: null, epoch);
}

public class RActionInvoker<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, TScrapbook, Task<RResult>> _func;
    
    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, TScrapbook, Task<RResult>> func,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
        ShutdownCoordinator shutdownCoordinator, 
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task<RResult> Invoke(TParam param)
    {
        var functionId = CreateFunctionId(param);
        var created = await PersistFunctionInStore(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
        var scrapbook = CreateScrapbook(functionId);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            RResult result;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                result = await _func(param, scrapbook);
            }
            catch (Exception exception)
            {
                await ProcessUnhandledException(functionId, exception, scrapbook);
                return new Fail(exception);
            }

            await ProcessResult(functionId, result, scrapbook);
            return result;
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }
    
    public async Task ScheduleInvocation(TParam param)
    {
        var functionId = CreateFunctionId(param);
        var created = await PersistFunctionInStore(functionId, param);
        if (!created) return;

        _ = Task.Run(async () =>
        {
            try
            {
                _shutdownCoordinator.RegisterRunningRFunc();
                using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
                var scrapbook = CreateScrapbook(functionId);

                RResult result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _func(param, scrapbook);
                }
                catch (Exception exception)
                {
                    await ProcessUnhandledException(functionId, exception, scrapbook);
                    return;
                }

                await ProcessResult(functionId, result, scrapbook);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(exception); }
            finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
        });
    }
    
    public async Task<RResult> ReInvoke(string functionInstanceId, IEnumerable<Status> expectedStatuses)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var (param, scrapbook, epoch) = await PrepareForReInvocation(functionId, expectedStatuses);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            RResult result;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                result = await _func(param, scrapbook);
            }
            catch (Exception exception)
            {
                await ProcessUnhandledException(functionId, exception, scrapbook, epoch);
                return new Fail(exception);
            }

            await ProcessResult(functionId, result, scrapbook, epoch);
            return result;
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, typeof(TScrapbook));

    private TScrapbook CreateScrapbook(FunctionId functionId)
        => _commonInvoker.CreateScrapbook<TScrapbook>(functionId, expectedEpoch: 0);

    private async Task<RResult> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionResult(functionId);

    private async Task<Tuple<TParam, TScrapbook, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses) 
        => await _commonInvoker.PrepareForReInvocation<TParam, TScrapbook>(functionId, expectedStatuses);
    
    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, TScrapbook scrapbook, int epoch = 0) 
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook, epoch);

    private async Task ProcessResult(FunctionId functionId, RResult result, TScrapbook scrapbook, int epoch = 0)
        => await _commonInvoker.ProcessResult(functionId, result, scrapbook, epoch);
}

