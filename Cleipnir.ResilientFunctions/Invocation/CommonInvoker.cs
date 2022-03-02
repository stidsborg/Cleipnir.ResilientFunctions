using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Invocation;

internal class CommonInvoker
{
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly ISerializer _serializer;
    private readonly IFunctionStore _functionStore;

    public CommonInvoker(ISerializer serializer, IFunctionStore functionStore, ShutdownCoordinator shutdownCoordinator)
    {
        _shutdownCoordinator = shutdownCoordinator;
        _serializer = serializer;
        _functionStore = functionStore;
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
    
    public async Task<RResult<TReturn>> WaitForFunctionResult<TReturn>(FunctionId functionId) //todo consider if this function should accept an epoch parameter
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
                    return new RResult<TReturn>(
                        functionId,
                        Outcome.Succeeded,
                        successResult: (TReturn) storedFunction.Result!.Deserialize(_serializer)!,
                        postponedUntil: null,
                        failedException: null
                    );
                case Status.Failed:
                    var error = _serializer.DeserializeError(storedFunction.ErrorJson!);
                    return new RResult<TReturn>(
                        functionId,
                        Outcome.Failed,
                        successResult: default,
                        postponedUntil: null,
                        failedException: new PreviousFunctionInvocationException(
                            functionId,
                            error,
                            $"'{functionId}' function invocation previously failed"
                        )
                    );
                case Status.Postponed:
                    return new RResult<TReturn>(
                        functionId,
                        Outcome.Postponed,
                        successResult: default,
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc),
                        failedException: null
                    );
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
                        functionId,
                        Outcome.Succeeded,
                        postponedUntil: null,
                        failedException: null
                    );
                case Status.Failed:
                    var error = _serializer.DeserializeError(storedFunction.ErrorJson!);
                    return new RResult(
                        functionId,
                        Outcome.Failed,
                        postponedUntil: null,
                        failedException: new PreviousFunctionInvocationException(
                            functionId,
                            error,
                            $"'{functionId}' function invocation previously failed"
                        )
                    );
                case Status.Postponed:
                    return new RResult(
                        functionId,
                        Outcome.Postponed,
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc),
                        failedException: null
                    );
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
    
    public async Task<RResult> ProcessReturned(
        FunctionId functionId, Return returned,
        RScrapbook? scrapbook, int expectedEpoch)
    {
        var scrapbookJson = scrapbook == null
            ? null
            : _serializer.SerializeScrapbook(scrapbook);
        bool success;
        switch (returned.Intent)
        {
            case Intent.Succeed:
                success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson,
                    result: null,
                    errorJson: null,
                    postponedUntil: null,
                    expectedEpoch
                );
                if (!success)
                    throw new FunctionInvocationException(
                        functionId,
                        $"Unable to persist function '{functionId}' result due to concurrent modification"
                    );
                return new RResult(functionId, Outcome.Succeeded, postponedUntil: null, failedException: null);
            case Intent.Postpone:
                success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson,
                    result: null,
                    errorJson: null,
                    returned.Postpone!.Value.Ticks,
                    expectedEpoch
                );
                if (!success)
                    throw new FunctionInvocationException(
                        functionId,
                        $"Unable to persist function '{functionId}' result due to concurrent modification"
                    );
                return new RResult(functionId, Outcome.Postponed, postponedUntil: returned.Postpone.Value,
                    failedException: null);
            case Intent.Fail:
                success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson,
                    result: null,
                    errorJson: _serializer.SerializeError(returned.Fail!.ToError()),
                    postponedUntil: null,
                    expectedEpoch
                );
                if (!success)
                    throw new FunctionInvocationException(
                        functionId,
                        $"Unable to persist function '{functionId}' result due to concurrent modification"
                    );
                return new RResult(functionId, Outcome.Failed, postponedUntil: null, failedException: returned.Fail);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
    
    public async Task<RResult<TReturn>> ProcessReturned<TReturn>(
        FunctionId functionId, 
        Return<TReturn> returned, 
        RScrapbook? scrapbook,
        int expectedEpoch)
    {
        var scrapbookJson = scrapbook == null
            ? null
            : _serializer.SerializeScrapbook(scrapbook);
        
        bool success;
        switch (returned.Intent)
        {
            case Intent.Succeed:
                success= await _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson,
                    result: new StoredResult(
                        ResultJson: returned.SucceedWithValue == null
                            ? null
                            : _serializer.SerializeResult(returned.SucceedWithValue),
                        ResultType: returned.SucceedWithValue?.GetType().SimpleQualifiedName()
                    ),
                    errorJson: null,
                    postponedUntil: null,
                    expectedEpoch
                );
                if (!success)
                    throw new FunctionInvocationException(
                        functionId,
                        $"Unable to persist function '{functionId}' result due to concurrent modification"
                    );
                return new RResult<TReturn>(
                    functionId,
                    Outcome.Succeeded,
                    returned.SucceedWithValue,
                    postponedUntil: null,
                    failedException: null
                );
            case Intent.Postpone:
                success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson,
                    result: null,
                    errorJson: null,
                    postponedUntil: returned.Postpone!.Value.Ticks,
                    expectedEpoch
                );
                if (!success)
                    throw new FunctionInvocationException(
                        functionId,
                        $"Unable to persist function '{functionId}' result due to concurrent modification"
                    );
                return new RResult<TReturn>(
                    functionId, Outcome.Postponed,
                    successResult: default, postponedUntil: returned.Postpone, failedException: null
                );
            case Intent.Fail:
                success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson,
                    result: null,
                    errorJson: _serializer.SerializeError(returned.Fail!.ToError()),
                    postponedUntil: null,
                    expectedEpoch
                );
                if (!success)
                    throw new FunctionInvocationException(
                        functionId,
                        $"Unable to persist function '{functionId}' result due to concurrent modification"
                    );
                return new RResult<TReturn>(
                    functionId, Outcome.Failed,
                    successResult: default, postponedUntil: null, failedException: returned.Fail
                );
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public async Task<Tuple<TParam, TScrapbook, int>> PrepareForReInvocation<TParam, TScrapbook>(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses) where TParam : notnull where TScrapbook : RScrapbook
    {
        var (param, epoch, scrapbook) = await PrepareForReInvocation<TParam>(functionId, expectedStatuses, hasScrapbook: true);
        return Tuple.Create(
            param, 
            (TScrapbook) scrapbook!,
            epoch
        );
    }

    public async Task<Tuple<TParam, int>> PrepareForReInvocation<TParam>(FunctionId functionId, IEnumerable<Status> expectedStatuses)
        where TParam : notnull
    {
        var (param, epoch, _) = await PrepareForReInvocation<TParam>(functionId, expectedStatuses, hasScrapbook: false);
        return Tuple.Create(param, epoch);
    }
    
    private async Task<Tuple<TParam, int, RScrapbook?>> PrepareForReInvocation<TParam>(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses,
        bool hasScrapbook)
        where TParam : notnull
    {
        expectedStatuses = expectedStatuses.ToList();
        var sf = await _functionStore.GetFunction(functionId);
        if (sf == null)
            throw new FunctionInvocationException(functionId, $"Function '{functionId}' not found");

        if (expectedStatuses.All(expectedStatus => expectedStatus != sf.Status))
            throw new FunctionInvocationException(functionId, $"Function '{functionId}' did not have expected status: '{sf.Status}'");

        var epoch = sf.Epoch + 1;
        var success = await _functionStore.TryToBecomeLeader(
            functionId,
            Status.Executing,
            expectedEpoch: sf.Epoch,
            newEpoch: epoch
        );
        
        if (!success)
            throw new FunctionInvocationException(functionId, $"Unable to become leader for function: '{functionId}'"); //todo concurrent modification exception

        var param = (TParam) _serializer.DeserializeParameter(sf.Parameter.ParamJson, sf.Parameter.ParamType);
        if (!hasScrapbook)
            return Tuple.Create(param, epoch, default(RScrapbook));
        
        var scrapbook = _serializer.DeserializeScrapbook(
            sf.Scrapbook!.ScrapbookJson,
            sf.Scrapbook.ScrapbookType
        );
        scrapbook.Initialize(functionId, _functionStore, _serializer, epoch);
        
        return Tuple.Create(param, epoch, (RScrapbook?) scrapbook);
    }
}