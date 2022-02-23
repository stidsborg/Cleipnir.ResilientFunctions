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
    private readonly InnerAction<TParam> _inner;

    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly OnActionException<TParam> _exceptionHandler;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        InnerAction<TParam> inner,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
        ShutdownCoordinator shutdownCoordinator, 
        UnhandledExceptionHandler unhandledExceptionHandler, 
        OnActionException<TParam>? exceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _exceptionHandler = exceptionHandler ?? DefaultProcessUnhandledException;
    }

    public async Task<RResult> Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
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
                result = await _inner(param);
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

    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
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
                    result = await _inner(param);
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
                result = await _inner(param);
            }
            catch (Exception exception)
            {
                result = _exceptionHandler(exception, functionInstanceId, param);
            }

            await ProcessResult(functionId, result, epoch);
            return result;
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }
    
    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param) 
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task<RResult> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionResult(functionId);

    private async Task<Tuple<TParam, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        => await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses);

    private RResult DefaultProcessUnhandledException(Exception unhandledException, FunctionInstanceId functionInstanceId, TParam _)
    {
        _unhandledExceptionHandler.Invoke(
            new FunctionInvocationUnhandledException(
                $"Function {functionInstanceId} threw unhandled exception",
                unhandledException
            )
        );

        return new Fail(unhandledException);
    }

    private Task ProcessResult(FunctionId functionId, RResult result, int epoch = 0)
        => _commonInvoker.ProcessResult(functionId, result, scrapbook: null, epoch);
}

public class RActionInvoker<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerAction<TParam, TScrapbook> _inner;
    
    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly OnActionException<TParam, TScrapbook> _exceptionHandler;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        InnerAction<TParam, TScrapbook> inner,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
        ShutdownCoordinator shutdownCoordinator, 
        UnhandledExceptionHandler unhandledExceptionHandler, 
        OnActionException<TParam, TScrapbook>? exceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _exceptionHandler = exceptionHandler ?? DefaultProcessUnhandledException;
    }

    public async Task<RResult> Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
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
                result = await _inner(param, scrapbook);
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
    
    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
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
                    result = await _inner(param, scrapbook);
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
                result = await _inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                result = _exceptionHandler(exception, scrapbook, functionId.InstanceId, param);
            }

            await ProcessResult(functionId, result, scrapbook, epoch);
            return result;
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, typeof(TScrapbook));

    private TScrapbook CreateScrapbook(FunctionId functionId)
        => _commonInvoker.CreateScrapbook<TScrapbook>(functionId, expectedEpoch: 0);

    private async Task<RResult> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionResult(functionId);

    private async Task<Tuple<TParam, TScrapbook, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses) 
        => await _commonInvoker.PrepareForReInvocation<TParam, TScrapbook>(functionId, expectedStatuses);
    
    private RResult DefaultProcessUnhandledException(Exception unhandledException, TScrapbook scrapbook, FunctionInstanceId functionInstanceId, TParam _)
    {
        _unhandledExceptionHandler.Invoke(
            new FunctionInvocationUnhandledException(
                $"Function {functionInstanceId} threw unhandled exception",
                unhandledException
            )
        );

        return new Fail(unhandledException);
    }

    private async Task ProcessResult(FunctionId functionId, RResult result, TScrapbook scrapbook, int epoch = 0)
        => await _commonInvoker.ProcessResult(functionId, result, scrapbook, epoch);
}

