using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RActionInvoker<TParam> where TParam : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, Task<RResult>> _func;

    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, Task<RResult>> func,
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
    {
        while (true)
        {
            var possibleResult = await _functionStore.GetFunction(functionId);
            if (possibleResult == null)
                throw new FrameworkException($"Function {functionId} does not exist");

            switch (possibleResult.Status)
            {
                case Status.Executing:
                    await Task.Delay(100);
                    continue;
                case Status.Succeeded:
                    return new RResult(ResultType.Succeeded, postponedUntil: null, failedException: null);
                case Status.Failed:
                    return Fail.WithException(possibleResult.Failure!.Deserialize(_serializer));
                case Status.Postponed:
                    var postponedUntil = new DateTime(possibleResult.PostponedUntil!.Value, DateTimeKind.Utc);
                    return Postpone.Until(postponedUntil);
                default:
                    throw new ArgumentOutOfRangeException(); //todo framework exception
            }
        }
    }

    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException)
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook: null);

    private Task ProcessResult(FunctionId functionId, RResult result)
    {
        var persistInStoreTask = result.ResultType switch
        {
            ResultType.Succeeded =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson: null,
                    result: null,
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

public class RActionInvoker<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, TScrapbook, Task<RResult>> _func;

    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly CommonInvoker _commonInvoker;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, TScrapbook, Task<RResult>> func,
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

    public async Task<RResult> Invoke(TParam param)
    {
        try
        {
            _shutdownCoordinator.RegisterRunningRFunc();
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
    {
        var scrapbook = new TScrapbook();
        scrapbook.Initialize(functionId, _functionStore, _serializer, epoch: 0);
        return scrapbook;
    }

    private async Task<RResult> WaitForFunctionResult(FunctionId functionId)
    {
        while (true)
        {
            var possibleResult = await _functionStore.GetFunction(functionId);
            if (possibleResult == null)
                throw new FrameworkException($"Function {functionId} does not exist");

            switch (possibleResult.Status)
            {
                case Status.Executing:
                    await Task.Delay(100);
                    continue;
                case Status.Succeeded:
                    return new RResult(ResultType.Succeeded, postponedUntil: null, failedException: null);
                case Status.Failed:
                    return Fail.WithException(possibleResult.Failure!.Deserialize(_serializer));
                case Status.Postponed:
                    var postponedUntil = new DateTime(possibleResult.PostponedUntil!.Value, DateTimeKind.Utc);
                    return Postpone.Until(postponedUntil);
                default:
                    throw new ArgumentOutOfRangeException(); //todo framework exception
            }
        }
    }

    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, TScrapbook scrapbook) 
        => await _commonInvoker.ProcessUnhandledException(functionId, unhandledException, scrapbook);

    private async Task ProcessResult(FunctionId functionId, RResult result, TScrapbook scrapbook)
    {
        var persistInStoreTask = result.ResultType switch
        {
            ResultType.Succeeded =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson: _serializer.SerializeScrapbook(scrapbook),
                    result: null,
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

