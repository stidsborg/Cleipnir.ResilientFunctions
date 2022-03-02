using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RFuncInvoker<TParam, TReturn> where TParam : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerFunc<TParam, TReturn> _inner;

    private readonly CommonInvoker _commonInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly OnFuncException<TParam, TReturn> _exceptionHandler;

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TReturn> inner,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
        ShutdownCoordinator shutdownCoordinator, 
        UnhandledExceptionHandler unhandledExceptionHandler,
        OnFuncException<TParam, TReturn>? exceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _exceptionHandler = exceptionHandler ?? DefaultProcessUnhandledException;
    }

    public async Task<RResult<TReturn>> Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var created = await PersistFunctionInStore(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            Return<TReturn> returned;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                returned = await _inner(param);
            }
            catch (Exception exception)
            {
                returned = _exceptionHandler(exception, functionInstanceId, param);
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
                Return<TReturn> returned;
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
    
    public async Task<RResult<TReturn>> ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, epoch) = await PrepareForReInvocation(functionId, expectedStatuses);
        
        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            Return<TReturn> returned;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                returned = await _inner(param);
            }
            catch (Exception exception)
            {
                returned = _exceptionHandler(exception, instanceId, param);
            }

            return await ProcessReturned(functionId, returned, epoch);
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task<RResult<TReturn>> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<Tuple<TParam, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        => await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses);

    private Task<RResult<TReturn>> ProcessReturned(FunctionId functionId, Return<TReturn> returned, int epoch = 0)
        => _commonInvoker.ProcessReturned(functionId, returned, scrapbook: null, epoch);
    
    private Return<TReturn> DefaultProcessUnhandledException(Exception unhandledException, FunctionInstanceId functionInstanceId, TParam _)
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
}

public class RFuncInvoker<TParam, TScrapbook, TReturn> 
    where TParam : notnull 
    where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly InnerFunc<TParam, TScrapbook, TReturn> _inner;

    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly OnFuncException<TParam, TScrapbook, TReturn> _exceptionHandler;

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        InnerFunc<TParam, TScrapbook, TReturn> inner,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
        ShutdownCoordinator shutdownCoordinator, 
        UnhandledExceptionHandler unhandledExceptionHandler, 
        OnFuncException<TParam, TScrapbook, TReturn>? exceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _inner = inner;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _exceptionHandler = exceptionHandler ?? DefaultProcessUnhandledException;
    }

    public async Task<RResult<TReturn>> Invoke(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        var created = await PersistFunctionInStore(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
        var scrapbook = CreateScrapbook(functionId);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            Return<TReturn> returned;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                returned = await _inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                returned = _exceptionHandler(exception, scrapbook, functionId.InstanceId, param);
            }
            
            return await ProcessResult(functionId, returned, scrapbook);
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    public async Task ScheduleInvocation(string functionInstanceId, TParam param)
    {
        var functionId = new FunctionId(_functionTypeId, functionInstanceId);
        await PersistFunctionInStore(functionId, param);

        _ = Task.Run(async () =>
        {
            _shutdownCoordinator.RegisterRunningRFunc();
            try
            {
                using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
                var scrapbook = CreateScrapbook(functionId);
                Return<TReturn> result;
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
    
    public async Task<RResult<TReturn>> ReInvoke(string instanceId, IEnumerable<Status> expectedStatuses)
    {
        var functionId = new FunctionId(_functionTypeId, instanceId);
        var (param, scrapbook, epoch) = await PrepareForReInvocation(functionId, expectedStatuses);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            Return<TReturn> returned;
            try
            {
                // *** USER FUNCTION INVOCATION *** 
                returned = await _inner(param, scrapbook);
            }
            catch (Exception exception)
            {
                returned = _exceptionHandler(exception, scrapbook, instanceId, param);
            }

            return await ProcessResult(functionId, returned, scrapbook, epoch);
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    private TScrapbook CreateScrapbook(FunctionId functionId, int epoch = 0)
        => _commonInvoker.CreateScrapbook<TScrapbook>(functionId, epoch);

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param) 
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: typeof(TScrapbook));

    private async Task<RResult<TReturn>> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<Tuple<TParam, TScrapbook, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        => await _commonInvoker.PrepareForReInvocation<TParam, TScrapbook>(functionId, expectedStatuses);

    private async Task<RResult<TReturn>> ProcessResult(FunctionId functionId, Return<TReturn> result, TScrapbook scrapbook, int epoch = 0)
        => await _commonInvoker.ProcessReturned(functionId, result, scrapbook, epoch);
    
    private Return<TReturn> DefaultProcessUnhandledException(Exception unhandledException, TScrapbook _, FunctionInstanceId functionInstanceId, TParam __)
    {
        _unhandledExceptionHandler.Invoke(
            new InnerFunctionUnhandledException(
                new FunctionId(_functionTypeId, functionInstanceId),
                $"Function {functionInstanceId} threw unhandled exception",
                unhandledException
            )
        );

        return new Domain.Fail(unhandledException);
    }
}