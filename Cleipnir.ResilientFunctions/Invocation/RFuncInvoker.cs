using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;

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

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, Task<RResult<TReturn>>> func,
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
                    await ProcessUnhandledException(functionId, exception);
                    return;
                }

                await ProcessResult(functionId, result);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(exception); }
            finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
        });
    }
    
    public async Task<RResult<TReturn>> ReInvoke(
        string instanceId, 
        Action<TParam> initializer, 
        IEnumerable<Status> expectedStatuses)
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
                initializer(param);
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

    private async Task<RResult<TReturn>> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<Tuple<TParam, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        => await _commonInvoker.PrepareForReInvocation<TParam>(functionId, expectedStatuses);
    
    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, int epoch = 0)
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook: null, epoch);

    private Task ProcessResult(FunctionId functionId, RResult<TReturn> result, int epoch = 0)
        => _commonInvoker.ProcessResult(functionId, result, scrapbook: null, epoch);
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

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, TScrapbook, Task<RResult<TReturn>>> func,
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
                    await ProcessUnhandledException(functionId, exception, scrapbook);
                    return;
                }

                await ProcessResult(functionId, result, scrapbook);
            }
            catch (Exception exception) { _unhandledExceptionHandler.Invoke(exception); }
            finally { _shutdownCoordinator.RegisterRFuncCompletion(); }
        });
    }
    
    public async Task<RResult<TReturn>> ReInvoke(string instanceId, Action<TParam, TScrapbook> initializer, IEnumerable<Status> expectedStatuses)
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
                initializer(param, scrapbook);
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

    private TScrapbook CreateScrapbook(FunctionId functionId, int epoch = 0)
        => _commonInvoker.CreateScrapbook<TScrapbook>(functionId, epoch);

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param) 
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: typeof(TScrapbook));

    private async Task<RResult<TReturn>> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TReturn>(functionId);

    private async Task<Tuple<TParam, TScrapbook, int>> PrepareForReInvocation(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        => await _commonInvoker.PrepareForReInvocation<TParam, TScrapbook>(functionId, expectedStatuses);
    
    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, TScrapbook scrapbook, int epoch = 0)
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook, epoch);

    private async Task ProcessResult(FunctionId functionId, RResult<TReturn> result, TScrapbook scrapbook, int epoch = 0)
        => await _commonInvoker.ProcessResult(functionId, result, scrapbook, epoch);
}