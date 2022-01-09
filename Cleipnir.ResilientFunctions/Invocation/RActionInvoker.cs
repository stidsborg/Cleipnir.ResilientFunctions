using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Invocation;

public class RActionInvoker<TParam> where TParam : notnull
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly Func<TParam, object> _idFunc;
    private readonly Func<TParam, Task<RResult>> _func;

    private readonly IFunctionStore _functionStore;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, Task<RResult>> func,
        IFunctionStore functionStore,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
        UnhandledExceptionHandler unhandledExceptionHandler)
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _functionStore = functionStore;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task<RResult> Invoke(TParam param, Action? onPersisted = null)
    {
        var functionId = CreateFunctionId(param);
        var created = await PersistFunctionInStore(functionId, param, onPersisted);
        if (!created) return await WaitForFunctionResult(functionId);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, 0);
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

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param, Action? onPersisted)
    {
        var paramJson = param.ToJson();
        var paramType = param.GetType().SimpleQualifiedName();
        var created = await _functionStore.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            scrapbookType: null,
            initialEpoch: 0,
            initialSignOfLife: 0,
            initialStatus: Status.Executing
        );
        if (onPersisted != null)
            _ = Task.Run(onPersisted);
        return created;
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
                    return Fail.WithException(possibleResult.Failure!.Deserialize());
                case Status.Postponed:
                    var postponedUntil = new DateTime(possibleResult.PostponedUntil!.Value, DateTimeKind.Utc);
                    return Postpone.Until(postponedUntil);
                case Status.Barricaded:
                    throw new FunctionInvocationException($"Function '{functionId}' has been barricaded");
                default:
                    throw new ArgumentOutOfRangeException(); //todo framework exception
            }
        }
    }
    
    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException)
    {
        _unhandledExceptionHandler.Invoke(
            new FunctionInvocationException(
                $"Function {functionId} threw unhandled exception", 
                unhandledException
            )
        );
        
        await _functionStore.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson: null,
            result: null,
            failed: new StoredFailure(
                FailedJson: unhandledException.ToJson(),
                FailedType: unhandledException.SimpleQualifiedTypeName()
            ),
            postponedUntil: null,
            expectedEpoch: 0
        );
    }

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
                        FailedJson: result.FailedException.ToJson(),
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
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    internal RActionInvoker(
        FunctionTypeId functionTypeId,
        Func<TParam, object> idFunc,
        Func<TParam, TScrapbook, Task<RResult>> func,
        IFunctionStore functionStore,
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory,
        UnhandledExceptionHandler unhandledExceptionHandler
    )
    {
        _functionTypeId = functionTypeId;
        _idFunc = idFunc;
        _func = func;
        _functionStore = functionStore;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task<RResult> Invoke(TParam param, Action? onPersisted = null)
    {
        var functionId = CreateFunctionId(param);
        var created = await PersistFunctionInStore(functionId, param, onPersisted);
        if (!created) return await WaitForFunctionResult(functionId);

        using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, 0);
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

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private TScrapbook CreateScrapbook(FunctionId functionId)
    {
        var scrapbook = new TScrapbook();
        scrapbook.Initialize(functionId, _functionStore, epoch: 0);
        return scrapbook;
    }

    private async Task<bool> PersistFunctionInStore(FunctionId functionId, TParam param, Action? onPersisted)
    {
        var paramJson = param.ToJson();
        var paramType = param.GetType().SimpleQualifiedName();
        var created = await _functionStore.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            scrapbookType: typeof(TScrapbook).SimpleQualifiedName(),
            initialEpoch: 0,
            initialSignOfLife: 0,
            initialStatus: Status.Executing
        );
        if (onPersisted != null)
            _ = Task.Run(onPersisted);
        return created;
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
                    return Fail.WithException(possibleResult.Failure!.Deserialize());
                case Status.Postponed:
                    var postponedUntil = new DateTime(possibleResult.PostponedUntil!.Value, DateTimeKind.Utc);
                    return Postpone.Until(postponedUntil);
                case Status.Barricaded:
                    throw new FunctionInvocationException($"Function '{functionId}' has been barricaded");
                default:
                    throw new ArgumentOutOfRangeException(); //todo framework exception
            }
        }
    }
    
    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, TScrapbook scrapbook)
    {
        _unhandledExceptionHandler.Invoke(
            new FunctionInvocationException(
                $"Function {functionId} threw unhandled exception", 
                unhandledException)
        );
        
        await _functionStore.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbook.ToJson(),
            result: null,
            failed: new StoredFailure(
                FailedJson: unhandledException.ToJson(),
                FailedType: unhandledException.SimpleQualifiedTypeName()
            ),
            postponedUntil: null,
            expectedEpoch: 0
        );
    }

    private async Task ProcessResult(FunctionId functionId, RResult result, TScrapbook scrapbook)
    {
        var persistInStoreTask = result.ResultType switch
        {
            ResultType.Succeeded =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson: scrapbook.ToJson(),
                    result: null,
                    failed: null,
                    postponedUntil: null,
                    expectedEpoch: 0
                ),
            ResultType.Postponed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson: scrapbook.ToJson(),
                    result: null,
                    failed: null,
                    result.PostponedUntil!.Value.Ticks,
                    expectedEpoch: 0
                ),
            ResultType.Failed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson: scrapbook.ToJson(),
                    result: null,
                    failed: new StoredFailure(
                        FailedJson: result.FailedException.ToJson(),
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

