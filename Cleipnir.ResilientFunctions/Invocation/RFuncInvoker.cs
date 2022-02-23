using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RFuncInvoker<TParam, TReturn> where TParam : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, Task<RResult<TReturn>>> _func;

    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly OnFuncException<TParam, TReturn> _exceptionHandler;

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, Task<RResult<TReturn>>> func,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
        ShutdownCoordinator shutdownCoordinator, 
        UnhandledExceptionHandler unhandledExceptionHandler,
        OnFuncException<TParam, TReturn>? exceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _exceptionHandler = exceptionHandler ?? DefaultProcessUnhandledException;
    }

    public async Task<RResult<TReturn>> Invoke(TParam param)
    {
        var functionId = CreateFunctionId(param);
        var created = await PersistFunctionInStore(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            RResult<TReturn> result;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                result = await _func(param);
            }
            catch (Exception exception)
            {
                result = _exceptionHandler(exception, functionId.InstanceId, param);
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
                _shutdownCoordinator.RegisterRunningRFunc();
                using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
                RResult<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _func(param);
                }
                catch (Exception exception)
                {
                    result = _exceptionHandler(exception, functionId.InstanceId, param);
                }

                await ProcessResult(functionId, result);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(exception); }
            finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
        });
    }
    
    public async Task<RResult<TReturn>> ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, epoch) = await PrepareForReInvocation(functionId, expectedStatuses);
        
        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            RResult<TReturn> result;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                result = await _func(param);
            }
            catch (Exception exception)
            {
                result = _exceptionHandler(exception, instanceId, param);
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

    private async Task<RResult<TReturn>> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<Tuple<TParam, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        => await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses);

    private Task ProcessResult(FunctionId functionId, RResult<TReturn> result, int epoch = 0)
        => _commonInvoker.ProcessResult(functionId, result, scrapbook: null, epoch);
    
    private RResult<TReturn> DefaultProcessUnhandledException(Exception unhandledException, FunctionInstanceId functionInstanceId, TParam _)
    {
        _unhandledExceptionHandler.Invoke(
            new FunctionInvocationUnhandledException(
                $"Function {functionInstanceId} threw unhandled exception",
                unhandledException
            )
        );

        return new Fail(unhandledException);
    }
}

public class RFuncInvoker<TParam, TScrapbook, TReturn> 
    where TParam : notnull 
    where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, TScrapbook, Task<RResult<TReturn>>> _func;

    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly OnFuncException<TParam, TScrapbook, TReturn> _exceptionHandler;

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, TScrapbook, Task<RResult<TReturn>>> func,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
        ShutdownCoordinator shutdownCoordinator, 
        UnhandledExceptionHandler unhandledExceptionHandler, 
        OnFuncException<TParam, TScrapbook, TReturn>? exceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _exceptionHandler = exceptionHandler ?? DefaultProcessUnhandledException;
    }

    public async Task<RResult<TReturn>> Invoke(TParam param)
    {
        var functionId = CreateFunctionId(param);
        var created = await PersistFunctionInStore(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
        var scrapbook = CreateScrapbook(functionId);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            RResult<TReturn> result;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                result = await _func(param, scrapbook);
            }
            catch (Exception exception)
            {
                result = _exceptionHandler(exception, scrapbook, functionId.InstanceId, param);
            }

            await ProcessResult(functionId, result, scrapbook);
            return result;
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    public async Task ScheduleInvocation(TParam param)
    {
        var functionId = CreateFunctionId(param);
        await PersistFunctionInStore(functionId, param);

        _ = Task.Run(async () =>
        {
            try
            {
                _shutdownCoordinator.RegisterRunningRFunc();
                using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
                var scrapbook = CreateScrapbook(functionId);
                RResult<TReturn> result;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    result = await _func(param, scrapbook);
                }
                catch (Exception exception)
                {
                    result = _exceptionHandler(exception, scrapbook, functionId.InstanceId, param);
                }

                await ProcessResult(functionId, result, scrapbook);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(exception); }
            finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
        });
    }
    
    public async Task<RResult<TReturn>> ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, scrapbook, epoch) = await PrepareForReInvocation(functionId, expectedStatuses);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            RResult<TReturn> result;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                result = await _func(param, scrapbook);
            }
            catch (Exception exception)
            {
                result = _exceptionHandler(exception, scrapbook, instanceId, param);
            }

            await ProcessResult(functionId, result, scrapbook, epoch);
            return result;
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private TScrapbook CreateScrapbook(FunctionId functionId, int epoch = 0)
        => _commonInvoker.CreateScrapbook<TScrapbook>(functionId, epoch);

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param) 
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: typeof(TScrapbook));

    private async Task<RResult<TReturn>> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<Tuple<TParam, TScrapbook, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        => await _commonInvoker.PrepareForReInvocation<TParam, TScrapbook>(functionId, expectedStatuses);

    private async Task ProcessResult(FunctionId functionId, RResult<TReturn> result, TScrapbook scrapbook, int epoch = 0)
        => await _commonInvoker.ProcessResult(functionId, result, scrapbook, epoch);
    
    private RResult<TReturn> DefaultProcessUnhandledException(Exception unhandledException, TScrapbook _, FunctionInstanceId functionInstanceId, TParam __)
    {
        _unhandledExceptionHandler.Invoke(
            new FunctionInvocationUnhandledException(
                $"Function {functionInstanceId} threw unhandled exception",
                unhandledException
            )
        );

        return new Fail(unhandledException);
    }
}