using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RFuncInvoker<TParam, TResult> where TParam : notnull where TResult : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, Task<RResult<TResult>>> _func;

    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, Task<RResult<TResult>>> func,
        IFunctionStore functionStore,
        ISerializer serializer,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
        UnhandledExceptionHandler unhandledExceptionHandler, 
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _functionStore = functionStore;
        _serializer = serializer;
        _commonInvoker = new CommonInvoker(
            serializer,
            functionStore,
            unhandledExceptionHandler,
            shutdownCoordinator
        );
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task<RResult<TResult>> Invoke(TParam param)
    {
        var functionId = CreateFunctionId(param);
        var created = await PersistFunctionInStore(functionId, param);
        if (!created) return await WaitForFunctionResult(functionId);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
        _shutdownCoordinator.RegisterRunningRFunc();
        RResult<TResult> result;
        try
        {
            // *** START OF USER FUNCTION INVOCATION *** 
            result = await _func(param);
            // *** END OF INVOCATION ***
        }
        catch (Exception exception)
        {
            await ProcessUnhandledException(functionId, exception);
            return new Fail(exception);
        }
        finally { _shutdownCoordinator.RegisterRFuncCompletion(); }

        await ProcessResult(functionId, result);
        return result;
    }

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param)
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: null);

    private async Task<RResult<TResult>> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TResult>(functionId);

    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException)
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook: null);

    private Task ProcessResult(FunctionId functionId, RResult<TResult> result)
    {
        var persistInStoreTask = result.ResultType switch
        {
            ResultType.Succeeded =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson: null,
                    result: new StoredResult(
                        ResultJson: _serializer.SerializeResult(result.SuccessResult!), 
                        ResultType: result.SuccessResult!.GetType().SimpleQualifiedName()
                    ),
                    failed: null,
                    postponedUntil: null,
                    expectedEpoch: 0
                ),
            ResultType.Postponed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson: null,
                    result: null,
                    failed: null,
                    result.PostponedUntil!.Value.Ticks,
                    expectedEpoch: 0
                ),
            ResultType.Failed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson: null,
                    result: null,
                    failed: new StoredFailure(
                        FailedJson: _serializer.SerializeFault(result.FailedException!),
                        FailedType: result.FailedException!.SimpleQualifiedTypeName()
                    ),
                    postponedUntil: null,
                    expectedEpoch: 0
                ),
            _ => throw new ArgumentOutOfRangeException()
        };

        return persistInStoreTask;
    }
}

public class RFuncInvoker<TParam, TScrapbook, TResult> 
    where TParam : notnull 
    where TScrapbook : RScrapbook, new()
    where TResult : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, TScrapbook, Task<RResult<TResult>>> _func;

    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    internal RFuncInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, TScrapbook, Task<RResult<TResult>>> func,
        IFunctionStore functionStore,
        ISerializer serializer,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
        UnhandledExceptionHandler unhandledExceptionHandler, 
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _functionStore = functionStore;
        _serializer = serializer;
        _commonInvoker = new CommonInvoker(
            serializer,
            functionStore,
            unhandledExceptionHandler,
            shutdownCoordinator
        );
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task<RResult<TResult>> Invoke(TParam param)
    {
        try
        {
            _shutdownCoordinator.RegisterRunningRFunc();
            var functionId = CreateFunctionId(param);
            var created = await PersistFunctionInStore(functionId, param);
            if (!created) return await WaitForFunctionResult(functionId);
            
            using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch: 0);
            var scrapbook = CreateScrapbook(functionId);
            RResult<TResult> result;
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
        } finally{ _shutdownCoordinator.RegisterRFuncCompletion(); }
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
                RResult<TResult> result;
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
        });
    }

    public async Task<RResult<TResult>> ReInvoke(
        FunctionInstanceId functionInstanceId, 
        Action<TParam, TScrapbook> initializer,
        IEnumerable<Status> expectedStatuses)
    {
        try
        {
            _shutdownCoordinator.RegisterRunningRFunc();
            var functionId = new FunctionId(_functionTypeId, functionInstanceId);
            var (param, scrapbook) = await _commonInvoker.PreprocessReInvocation<TParam, TScrapbook>(functionId, expectedStatuses);

            initializer(param, scrapbook);
            await scrapbook.Save();
            
            RResult<TResult> result;
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
        } finally{ _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private TScrapbook CreateScrapbook(FunctionId functionId)
        => _commonInvoker.CreateScrapbook<TScrapbook>(functionId, expectedEpoch: 0);

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param) 
        => await _commonInvoker.PersistFunctionInStore(functionId, param, scrapbookType: typeof(TScrapbook));

    private async Task<RResult<TResult>> WaitForFunctionResult(FunctionId functionId)
        => await _commonInvoker.WaitForFunctionResult<TResult>(functionId);

    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, TScrapbook scrapbook)
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook);

    private async Task ProcessResult(FunctionId functionId, RResult<TResult> result, TScrapbook scrapbook)
    {
        var persistInStoreTask = result.ResultType switch
        {
            ResultType.Succeeded =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson: _serializer.SerializeScrapbook(scrapbook),
                    result: new StoredResult(
                        _serializer.SerializeResult(result.SuccessResult!), 
                        result.SuccessResult!.GetType().SimpleQualifiedName()
                    ),
                    failed: null,
                    postponedUntil: null,
                    expectedEpoch: 0
                ),
            ResultType.Postponed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson: _serializer.SerializeScrapbook(scrapbook),
                    result: null,
                    failed: null,
                    result.PostponedUntil!.Value.Ticks,
                    expectedEpoch: 0
                ),
            ResultType.Failed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson: _serializer.SerializeScrapbook(scrapbook),
                    result: null,
                    failed: new StoredFailure(
                        FailedJson: _serializer.SerializeFault(result.FailedException!),
                        FailedType: result.FailedException!.SimpleQualifiedTypeName()
                    ),
                    postponedUntil: null,
                    expectedEpoch: 0
                ),
            _ => throw new ArgumentOutOfRangeException()
        };

        var success = await persistInStoreTask;
        if (!success)
            throw new FrameworkException($"Unable to persist function '{functionId}' result in FunctionStore");
    }
}