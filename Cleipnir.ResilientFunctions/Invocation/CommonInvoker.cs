using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Invocation;

internal class CommonInvoker
{
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ISerializer _serializer;
    private readonly IFunctionStore _functionStore;

    public CommonInvoker(
        ISerializer serializer,
        IFunctionStore functionStore, 
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _shutdownCoordinator = shutdownCoordinator;
        _serializer = serializer;
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public async Task<bool> PersistFunctionInStore<TParam>(FunctionId functionId, TParam param, Type? scrapbookType)
        where TParam : notnull
    {
        if (_shutdownCoordinator.ShutdownInitiated)
            throw new ObjectDisposedException($"{nameof(RFunctions)} has been disposed");
        var paramJson = _serializer.SerializeParameter(param);
        var paramType = param.SimpleQualifiedTypeName();
        var created = await _functionStore.CreateFunction(
            functionId,
            param: new StoredParameter(paramJson, paramType),
            scrapbookType: scrapbookType?.SimpleQualifiedName(),
            initialEpoch: 0,
            initialSignOfLife: 0,
            initialStatus: Status.Executing
        );
        
        return created;
    }
    
    public async Task<RResult<TResult>> WaitForFunctionResult<TResult>(FunctionId functionId)
    {
        while (true)
        {
            var storedFunction = await _functionStore.GetFunction(functionId);
            if (storedFunction == null)
                throw new FrameworkException($"Function {functionId} does not exist");

            switch (storedFunction.Status)
            {
                case Status.Executing:
                    await Task.Delay(100);
                    continue;
                case Status.Succeeded:
                    return new RResult<TResult>(
                        ResultType.Succeeded,
                        successResult: (TResult) storedFunction.Result!.Deserialize(_serializer),
                        postponedUntil: null,
                        failedException: null
                    );
                case Status.Failed:
                    return Fail.WithException(storedFunction.Failure!.Deserialize(_serializer));
                case Status.Postponed:
                    var postponedUntil = new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc);
                    return Postpone.Until(postponedUntil);
                default:
                    throw new ArgumentOutOfRangeException(); //todo framework exception
            }
        }
    }
    
    public async Task<RResult> WaitForActionResult(FunctionId functionId)
    {
        while (true)
        {
            var storedFunction = await _functionStore.GetFunction(functionId);
            if (storedFunction == null)
                throw new FrameworkException($"Function {functionId} does not exist");

            switch (storedFunction.Status)
            {
                case Status.Executing:
                    await Task.Delay(100);
                    continue;
                case Status.Succeeded:
                    return new RResult(
                        ResultType.Succeeded,
                        postponedUntil: null,
                        failedException: null
                    );
                case Status.Failed:
                    return Fail.WithException(storedFunction.Failure!.Deserialize(_serializer));
                case Status.Postponed:
                    var postponedUntil = new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc);
                    return Postpone.Until(postponedUntil);
                default:
                    throw new ArgumentOutOfRangeException(); //todo framework exception
            }
        }
    }
    
    public TScrapbook CreateScrapbook<TScrapbook>(FunctionId functionId, int expectedEpoch) where TScrapbook : RScrapbook, new()
    {
        var scrapbook = new TScrapbook();
        scrapbook.Initialize(functionId, _functionStore, _serializer, expectedEpoch);
        return scrapbook;
    }
    
    public async Task ProcessResult(FunctionId functionId, RResult result, RScrapbook? scrapbook, int expectedEpoch)
    {
        var scrapbookJson = scrapbook == null
            ? null
            : _serializer.SerializeScrapbook(scrapbook);
        
        var persistInStoreTask = result.ResultType switch
        {
            ResultType.Succeeded =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson,
                    result: null,
                    failed: null,
                    postponedUntil: null,
                    expectedEpoch
                ),
            ResultType.Postponed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson,
                    result: null,
                    failed: null,
                    result.PostponedUntil!.Value.Ticks,
                    expectedEpoch
                ),
            ResultType.Failed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson,
                    result: null,
                    failed: new StoredFailure(
                        FailedJson: _serializer.SerializeFault(result.FailedException!),
                        FailedType: result.FailedException!.SimpleQualifiedTypeName()
                    ),
                    postponedUntil: null,
                    expectedEpoch
                ),
            _ => throw new ArgumentOutOfRangeException()
        };

        var success = await persistInStoreTask;
        if (!success)
            throw new FrameworkException($"Unable to persist function '{functionId}' result in FunctionStore");
    }
    
    public async Task ProcessResult<TResult>(
        FunctionId functionId, 
        RResult<TResult> result, 
        RScrapbook? scrapbook,
        int expectedEpoch)
    {
        var scrapbookJson = scrapbook == null
            ? null
            : _serializer.SerializeScrapbook(scrapbook);
        
        var persistInStoreTask = result.ResultType switch
        {
            ResultType.Succeeded =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson,
                    result: new StoredResult(
                        _serializer.SerializeResult(result.SuccessResult!), 
                        result.SuccessResult!.GetType().SimpleQualifiedName()
                    ),
                    failed: null,
                    postponedUntil: null,
                    expectedEpoch
                ),
            ResultType.Postponed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson,
                    result: null,
                    failed: null,
                    result.PostponedUntil!.Value.Ticks,
                    expectedEpoch
                ),
            ResultType.Failed =>
                _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson,
                    result: null,
                    failed: new StoredFailure(
                        FailedJson: _serializer.SerializeFault(result.FailedException!),
                        FailedType: result.FailedException!.SimpleQualifiedTypeName()
                    ),
                    postponedUntil: null,
                    expectedEpoch
                ),
            _ => throw new ArgumentOutOfRangeException()
        };

        var success = await persistInStoreTask;
        if (!success)
            throw new FrameworkException($"Unable to persist function '{functionId}' result in FunctionStore");
    }
    
    public async Task ProcessUnhandledException(FunctionId functionId, Exception unhandledException, RScrapbook? scrapbook)
    {
        _unhandledExceptionHandler.Invoke(new FunctionInvocationUnhandledException(
            $"Function {functionId} threw unhandled exception", 
            unhandledException)
        );
        
        await _functionStore.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson: scrapbook == null ? null : _serializer.SerializeScrapbook(scrapbook),
            result: null,
            failed: new StoredFailure(
                FailedJson: _serializer.SerializeFault(unhandledException),
                FailedType: unhandledException.SimpleQualifiedTypeName()
            ),
            postponedUntil: null,
            expectedEpoch: 0
        );
    }

    public async Task<(TParam, TScrapbook)> PreprocessReInvocation<TParam, TScrapbook>(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        where TParam : notnull where TScrapbook : RScrapbook
    {
        expectedStatuses = expectedStatuses.ToList();
        var sf = await _functionStore.GetFunction(functionId);
        if (sf == null)
            throw new FunctionInvocationException($"Function '{functionId}' not found");

        if (expectedStatuses.All(expectedStatus => expectedStatus != sf.Status))
            throw new FunctionInvocationException($"Function '{functionId}' did not have expected status: '{sf.Status}'");

        var epoch = sf.Epoch + 1;
        var success = await _functionStore.TryToBecomeLeader(functionId, sf.Status, sf.Epoch, epoch);
        if (!success)
            throw new FunctionInvocationException($"Unable to become leader for function: '{functionId}'");
        
        sf = await _functionStore.GetFunction(functionId);
        if (sf == null)
            throw new FunctionInvocationException($"Function '{functionId}' not found");
            
        if (expectedStatuses.All(expectedStatus => expectedStatus != sf.Status))
            throw new FunctionInvocationException($"Function '{functionId}' did not have expected status: '{sf.Status}'");

        var param = (TParam) _serializer.DeserializeParameter(sf.Parameter.ParamJson, sf.Parameter.ParamType);
        var scrapbook = (TScrapbook) _serializer.DeserializeScrapbook(
            sf.Scrapbook!.ScrapbookJson,
            sf.Scrapbook.ScrapbookType
        ); 

        scrapbook.Initialize(functionId, _functionStore, _serializer, epoch);
        
        return new(param, scrapbook);
    }
}