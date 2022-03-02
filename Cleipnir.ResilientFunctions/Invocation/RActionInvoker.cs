using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
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
            Return returned;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                returned = await _inner(param);
            }
            catch (Exception exception)
            {
                returned = _exceptionHandler(exception, functionId.InstanceId, param);
            }

            return await ProcessReturned(functionId, returned);
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
            _shutdownCoordinator.RegisterRunningRFunc();
            try
            {
                using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
                Return returned;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    returned = await _inner(param);
                }
                catch (Exception exception)
                {
                    returned = _exceptionHandler(exception, functionId.InstanceId, param);
                }

                await ProcessReturned(functionId, returned);
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
            Return returned;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                returned = await _inner(param);
            }
            catch (Exception exception)
            {
                returned = _exceptionHandler(exception, functionInstanceId, param);
            }

            return await ProcessReturned(functionId, returned, epoch);
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }
    
    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param) 
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task<RResult> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionResult(functionId);

    private async Task<Tuple<TParam, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        => await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses);

    private Return DefaultProcessUnhandledException(Exception unhandledException, FunctionInstanceId functionInstanceId, TParam _)
    {
        _unhandledExceptionHandler.Invoke(
            new InnerFunctionUnhandledException(
                new FunctionId(_functionTypeId, functionInstanceId),
                $"Function {functionInstanceId} threw unhandled exception",
                unhandledException
            )
        );

        return new Fail(unhandledException);
    }

    private Task<RResult> ProcessReturned(FunctionId functionId, Return returned, int epoch = 0)
        => _commonInvoker.ProcessReturned(functionId, returned, scrapbook: null, epoch);
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
            Return result;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                result = await _inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                result = _exceptionHandler(exception, scrapbook, functionId.InstanceId, param);
            }

            return await ProcessReturned(functionId, result, scrapbook);
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
            _shutdownCoordinator.RegisterRunningRFunc();
            try
            {
                using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
                var scrapbook = CreateScrapbook(functionId);

                Return returned;
                try
                {
                    // *** USER FUNCTION INVOCATION *** 
                    returned = await _inner(param, scrapbook);
                }
                catch (Exception exception)
                {
                    returned = _exceptionHandler(exception, scrapbook, functionId.InstanceId, param);
                }

                await ProcessReturned(functionId, returned, scrapbook);
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
            Return returned;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                returned = await _inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                returned = _exceptionHandler(exception, scrapbook, functionId.InstanceId, param);
            }

            return await ProcessReturned(functionId, returned, scrapbook, epoch);
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
    
    private Return DefaultProcessUnhandledException(Exception unhandledException, TScrapbook scrapbook, FunctionInstanceId functionInstanceId, TParam _)
    {
        _unhandledExceptionHandler.Invoke(
            new InnerFunctionUnhandledException(
                new FunctionId(_functionTypeId, functionInstanceId),
                $"Function {functionInstanceId} threw unhandled exception",
                unhandledException
            )
        );

        return new Fail(unhandledException);
    }

    private async Task<RResult> ProcessReturned(FunctionId functionId, Return returned, TScrapbook scrapbook, int epoch = 0)
        => await _commonInvoker.ProcessReturned(functionId, returned, scrapbook, epoch);
}

