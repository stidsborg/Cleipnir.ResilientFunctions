using System;
using System.Linq;
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
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task<RResult<TResult>> Invoke(TParam param, Action? onPersisted = null, bool reInvoke = false, Status[]? onlyReInvokeWhen = null)
    {
        try
        {
            _shutdownCoordinator.RegisterRunningRFunc();
            var functionId = CreateFunctionId(param);
            var (created, epoch) = await PersistFunctionInStore(functionId, param, onPersisted, reInvoke, onlyReInvokeWhen);
            if (!created) return await WaitForFunctionResult(functionId);

            using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
            RResult<TResult> result;
            try
            {
                //USER FUNCTION INVOCATION! 
                result = await _func(param);
            }
            catch (Exception exception)
            {
                await ProcessUnhandledException(functionId, exception, epoch);
                return new Fail(exception);
            }

            await ProcessResult(functionId, result, epoch);
            return result;    
        } finally{ _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());

    private async Task<CreatedAndEpoch> PersistFunctionInStore(FunctionId functionId, TParam param, Action? onPersisted, bool reInvoke, Status[]? onlyReInvokeWhen)
    {
        if (_shutdownCoordinator.ShutdownInitiated)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        var epoch = 0;
        var paramJson = _serializer.SerializeParameter(param);
        var paramType = param.GetType().SimpleQualifiedName();
        var created = await _functionStore.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            scrapbookType: null,
            initialEpoch: epoch,
            initialSignOfLife: 0,
            initialStatus: Status.Executing
        );
        
        if (!created && reInvoke)
        {
            onlyReInvokeWhen ??= new[] {Status.Failed};
            var storedFunction = await _functionStore.GetFunction(functionId);
            if (storedFunction == null)
                throw new FrameworkException($"Function '{functionId}' not found on re-invocation");
            if (onlyReInvokeWhen.All(status => storedFunction.Status != status))
                throw new FrameworkException("Unable to re-invoke function '{functionId}' as it is in unexpected state");
            epoch = storedFunction.Epoch + 1;
            var success = await _functionStore.TryToBecomeLeader(
                functionId,
                Status.Executing,
                expectedEpoch: epoch - 1,
                newEpoch: epoch
            );
            created = success;
        }
        
        if (onPersisted != null)
            _ = Task.Run(onPersisted);
        
        return new CreatedAndEpoch(created, epoch);
    }
    private record CreatedAndEpoch(bool Created, int Epoch);

    private async Task<RResult<TResult>> WaitForFunctionResult(FunctionId functionId)
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
                    return new RResult<TResult>(
                        ResultType.Succeeded,
                        successResult: (TResult) possibleResult.Result!.Deserialize(_serializer),
                        postponedUntil: null,
                        failedException: null
                    );
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
    
    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, int epoch)
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
                FailedJson: _serializer.SerializeFault(unhandledException),
                FailedType: unhandledException.SimpleQualifiedTypeName()
            ),
            postponedUntil: null,
            expectedEpoch: epoch
        );
    }

    private Task ProcessResult(FunctionId functionId, RResult<TResult> result, int epoch)
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
                    expectedEpoch: epoch
                ),
            ResultType.Postponed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson: null,
                    result: null,
                    failed: null,
                    result.PostponedUntil!.Value.Ticks,
                    expectedEpoch: epoch
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
                    expectedEpoch: epoch
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
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task<RResult<TResult>> Invoke(TParam param, Action? onPersisted = null, bool reInvoke = false, Status[]? onlyReInvokeWhen = null)
    {
        try
        {
            _shutdownCoordinator.RegisterRunningRFunc();
            var functionId = CreateFunctionId(param);
            var (created, epoch, scrapbook) = await PersistFunctionInStore(functionId, param, onPersisted, reInvoke, onlyReInvokeWhen);
            if (!created) return await WaitForFunctionResult(functionId);

            using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
            RResult<TResult> result;
            try
            {
                //USER FUNCTION INVOCATION! 
                result = await _func(param, scrapbook);
            }
            catch (Exception exception)
            {
                await ProcessUnhandledException(functionId, exception, scrapbook, epoch);
                return new Fail(exception);
            }

            await ProcessResult(functionId, result, scrapbook, epoch);
            return result;
        } finally{ _shutdownCoordinator.RegisterRFuncCompletion(); }
    }

    private FunctionId CreateFunctionId(TParam param)
        => new FunctionId(_functionTypeId, _idFunc(param).ToString()!.ToFunctionInstanceId());
    
    private TScrapbook CreateScrapbook(FunctionId functionId, int epoch)
    {
        var scrapbook = new TScrapbook();
        scrapbook.Initialize(functionId, _functionStore, _serializer, epoch);
        return scrapbook;
    }

    private async Task<CreatedAndEpochAndScrapbook> PersistFunctionInStore(FunctionId functionId, TParam param, Action? onPersisted, bool reInvoke, Status[]? onlyReInvokeWhen)
    {
        if (_shutdownCoordinator.ShutdownInitiated)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        var epoch = 0;
        TScrapbook? scrapbook = null;
        var paramJson = _serializer.SerializeParameter(param);
        var paramType = param.GetType().SimpleQualifiedName();
        var created = await _functionStore.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            scrapbookType: typeof(TScrapbook).SimpleQualifiedName(),
            initialEpoch: epoch,
            initialSignOfLife: 0,
            initialStatus: Status.Executing
        );
        if (!created && reInvoke)
        {
            onlyReInvokeWhen ??= new[] {Status.Failed};
            var storedFunction = await _functionStore.GetFunction(functionId);
            if (storedFunction == null)
                throw new FrameworkException($"Function '{functionId}' not found on re-invocation");
            if (onlyReInvokeWhen.All(status => storedFunction.Status != status))
                throw new FrameworkException($"Unable to re-invoke function '{functionId}' as it is in unexpected state");
            epoch = storedFunction.Epoch + 1;
            scrapbook = (TScrapbook) _serializer.DeserializeScrapbook(
                storedFunction.Scrapbook!.ScrapbookJson,
                storedFunction.Scrapbook.ScrapbookType
            );
            var success = await _functionStore.TryToBecomeLeader(
                functionId,
                Status.Executing,
                expectedEpoch: epoch - 1,
                newEpoch: epoch
            );
            created = success;
        }
        
        scrapbook ??= CreateScrapbook(functionId, epoch);
        
        if (onPersisted != null)
            _ = Task.Run(onPersisted);
        
        return new CreatedAndEpochAndScrapbook(created, epoch, scrapbook);
    }

    private record CreatedAndEpochAndScrapbook(bool Created, int Epoch, TScrapbook Scrapbook);

    private async Task<RResult<TResult>> WaitForFunctionResult(FunctionId functionId)
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
                    return new RResult<TResult>(
                        ResultType.Succeeded, 
                        successResult: (TResult) possibleResult.Result!.Deserialize(_serializer), 
                        postponedUntil: null, 
                        failedException: null
                    );
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
    
    private async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, TScrapbook scrapbook, int epoch)
    {
        _unhandledExceptionHandler.Invoke(new FunctionInvocationException(
            $"Function {functionId} threw unhandled exception", 
            unhandledException)
        );
        
        await _functionStore.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson: _serializer.SerializeScrapbook(scrapbook),
            result: null,
            failed: new StoredFailure(
                FailedJson: _serializer.SerializeFault(unhandledException),
                FailedType: unhandledException.SimpleQualifiedTypeName()
            ),
            postponedUntil: null,
            expectedEpoch: epoch
        );
    }

    private async Task ProcessResult(FunctionId functionId, RResult<TResult> result, TScrapbook scrapbook, int epoch)
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
                    expectedEpoch: epoch
                ),
            ResultType.Postponed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson: _serializer.SerializeScrapbook(scrapbook),
                    result: null,
                    failed: null,
                    result.PostponedUntil!.Value.Ticks,
                    expectedEpoch: epoch
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
                    expectedEpoch: epoch
                ),
            _ => throw new ArgumentOutOfRangeException()
        };

        var success = await persistInStoreTask;
        if (!success)
            throw new FrameworkException($"Unable to persist function '{functionId}' result in FunctionStore");
    }
}