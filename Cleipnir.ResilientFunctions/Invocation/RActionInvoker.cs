using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
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

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, Task<RResult>> func,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task<RResult> Invoke(TParam param)
    {
        try
        {
            _shutdownCoordinator.RegisterRunningRFunc();
            var functionId = CreateFunctionId(param);
            var created = await PersistFunctionInStore(functionId, param);
            if (!created) return await WaitForFunctionResult(functionId);

            using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
            RResult result;
            try
            {
                //USER FUNCTION INVOCATION! 
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

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param) 
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task<RResult> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionResult(functionId);

    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException)
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook: null);

    private Task ProcessResult(FunctionId functionId, RResult result)
        => _commonInvoker.ProcessResult(functionId, result, scrapbook: null, expectedEpoch: 0);
}

public class RActionInvoker<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, TScrapbook, Task<RResult>> _func;
    
    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, TScrapbook, Task<RResult>> func,
        CommonInvoker commonInvoker,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _commonInvoker = commonInvoker;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task<RResult> Invoke(TParam param)
    {
        try
        {
            _shutdownCoordinator.RegisterRunningRFunc(); //todo move this further down in the method
            var functionId = CreateFunctionId(param);
            var created = await PersistFunctionInStore(functionId, param);
            if (!created) return await WaitForFunctionResult(functionId);

            using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);

            var scrapbook = CreateScrapbook(functionId);
            RResult result;
            try
            {
                //USER FUNCTION INVOCATION! 
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

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, typeof(TScrapbook));

    private TScrapbook CreateScrapbook(FunctionId functionId)
        => _commonInvoker.CreateScrapbook<TScrapbook>(functionId, expectedEpoch: 0);

    private async Task<RResult> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForActionResult(functionId);

    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, TScrapbook scrapbook) 
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook);

    private async Task ProcessResult(FunctionId functionId, RResult result, TScrapbook scrapbook)
        => await _commonInvoker.ProcessResult(functionId, result, scrapbook, expectedEpoch: 0);
}

