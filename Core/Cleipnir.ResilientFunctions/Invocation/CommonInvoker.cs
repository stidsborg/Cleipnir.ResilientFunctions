using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Invocation;

internal class CommonInvoker
{
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly ISerializer _serializer;
    private readonly IFunctionStore _functionStore;
    private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

    public CommonInvoker(
        ISerializer serializer, 
        IFunctionStore functionStore, 
        ShutdownCoordinator shutdownCoordinator, 
        SignOfLifeUpdaterFactory signOfLifeUpdaterFactory)
    {
        _shutdownCoordinator = shutdownCoordinator;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _serializer = serializer;
        _functionStore = functionStore;
    }

    public async Task<Tuple<bool, IDisposable>> PersistFunctionInStore<TParam>(FunctionId functionId, TParam param, Type? scrapbookType)
        where TParam : notnull
    {
        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        var paramJson = _serializer.SerializeParameter(param);
        var paramType = param.SimpleQualifiedTypeName();
        try
        {
            var created = await _functionStore.CreateFunction(
                functionId,
                param: new StoredParameter(paramJson, paramType),
                scrapbookType: scrapbookType?.SimpleQualifiedName(),
                initialEpoch: 0,
                initialSignOfLife: 0,
                initialStatus: Status.Executing
            );

            if (!created) runningFunction.Dispose();
            return Tuple.Create(created, runningFunction);
        }
        catch (Exception)
        {
            runningFunction.Dispose();
            throw;
        }
    }
    
    public async Task<TReturn> WaitForFunctionResult<TReturn>(FunctionId functionId) //todo consider if this function should accept an epoch parameter
    {
        while (true)
        {
            var storedFunction = await _functionStore.GetFunction(functionId);
            if (storedFunction == null)
                throw new FrameworkException(functionId.TypeId, $"Function {functionId} does not exist");

            switch (storedFunction.Status)
            {
                case Status.Executing:
                    await Task.Delay(100);
                    continue;
                case Status.Succeeded:
                    return (TReturn) storedFunction.Result!.Deserialize(_serializer)!;
                case Status.Failed:
                    var error = _serializer.DeserializeError(storedFunction.ErrorJson!);
                    throw new PreviousFunctionInvocationException(functionId, error);
                case Status.Postponed:
                    throw new FunctionInvocationPostponedException(
                        functionId,
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc)
                    );
                default:
                    throw new ArgumentOutOfRangeException(); //todo framework exception
            }
        }
    }
    
    public async Task WaitForActionCompletion(FunctionId functionId)
    {
        while (true)
        {
            var storedFunction = await _functionStore.GetFunction(functionId);
            if (storedFunction == null)
                throw new FrameworkException(functionId.TypeId, $"Function {functionId} does not exist");

            switch (storedFunction.Status)
            {
                case Status.Executing:
                    await Task.Delay(100);
                    continue;
                case Status.Succeeded:
                    return;
                case Status.Failed:
                    var error = _serializer.DeserializeError(storedFunction.ErrorJson!);
                    throw new PreviousFunctionInvocationException(functionId, error);
                case Status.Postponed:
                    throw new FunctionInvocationPostponedException(
                        functionId,
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc)
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

    public async Task PersistFailure(FunctionId functionId, Exception exception, RScrapbook? scrapbook, int expectedEpoch)
    {
        var scrapbookJson = scrapbook == null
            ? null
            : _serializer.SerializeScrapbook(scrapbook);
        
        var success = await _functionStore.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson,
            result: null,
            errorJson: _serializer.SerializeError(exception.ToError()),
            postponedUntil: null,
            expectedEpoch
        );
        if (!success) throw new ConcurrentModificationException(functionId);
    }
    
    public async Task PersistResult(
        FunctionId functionId,
        Result result,
        RScrapbook? scrapbook,
        int expectedEpoch)
    {
        var scrapbookJson = scrapbook == null
            ? null
            : _serializer.SerializeScrapbook(scrapbook);

        switch (result.Outcome)
        {
            case Outcome.Succeed:
                var success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson,
                    result: null,
                    errorJson: null,
                    postponedUntil: null,
                    expectedEpoch
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
            case Outcome.Postpone:
                success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson,
                    result: null,
                    errorJson: null,
                    postponedUntil: result.Postpone!.DateTime.Ticks,
                    expectedEpoch
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
            case Outcome.Fail:
                success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson,
                    result: null,
                    errorJson: _serializer.SerializeError(result.Fail!.ToError()),
                    postponedUntil: null,
                    expectedEpoch
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public async Task PersistResult<TReturn>(
        FunctionId functionId, 
        Result<TReturn> result, 
        RScrapbook? scrapbook,
        int expectedEpoch)
    {
        var scrapbookJson = scrapbook == null
            ? null
            : _serializer.SerializeScrapbook(scrapbook);
        
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                var success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson,
                    result: new StoredResult(
                        ResultJson: result.SucceedWithValue == null
                            ? null
                            : _serializer.SerializeResult(result.SucceedWithValue),
                        ResultType: result.SucceedWithValue?.GetType().SimpleQualifiedName()
                    ),
                    errorJson: null,
                    postponedUntil: null,
                    expectedEpoch
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
            case Outcome.Postpone:
                success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson,
                    result: null,
                    errorJson: null,
                    postponedUntil: result.Postpone!.DateTime.Ticks,
                    expectedEpoch
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
            case Outcome.Fail:
                success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson,
                    result: null,
                    errorJson: _serializer.SerializeError(result.Fail!.ToError()),
                    postponedUntil: null,
                    expectedEpoch
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public static void EnsureSuccess(FunctionId functionId, Result result, bool allowPostponed)
    {
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return;
            case Outcome.Postpone:
                if (allowPostponed)
                    return;
                else 
                    throw new FunctionInvocationPostponedException(functionId, result.Postpone!.DateTime);
            case Outcome.Fail:
                throw result.Fail!;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public static void EnsureSuccess<TReturn>(FunctionId functionId, Result<TReturn> result, bool allowPostponed)
    {
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return;
            case Outcome.Postpone:
                if (allowPostponed)
                    return;
                else
                    throw new FunctionInvocationPostponedException(functionId, result.Postpone!.DateTime);
            case Outcome.Fail:
                throw result.Fail!;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public async Task<PreparedReInvocation<TParam, TScrapbook>> PrepareForReInvocation<TParam, TScrapbook>(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        var (param, epoch, scrapbook, runningFunction) = await PrepareForReInvocation<TParam>(
            functionId,
            expectedStatuses,
            expectedEpoch,
            hasScrapbook: true
        );
        return new PreparedReInvocation<TParam, TScrapbook>(
            param,
            epoch, 
            (TScrapbook) scrapbook!,
            runningFunction
        );
    }

    public async Task<PreparedReInvocation<TParam>> PrepareForReInvocation<TParam>(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch)
        where TParam : notnull
    {
        var (param, epoch, _, runningFunction) = await PrepareForReInvocation<TParam>(
            functionId,
            expectedStatuses,
            expectedEpoch,
            hasScrapbook: false
        );
        return new PreparedReInvocation<TParam>(param, epoch, runningFunction);
    }
    
    private async Task<PreparedReInvocation<TParam, RScrapbook>> PrepareForReInvocation<TParam>(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch,
        bool hasScrapbook)
        where TParam : notnull
    {
        expectedStatuses = expectedStatuses.ToList();
        var sf = await _functionStore.GetFunction(functionId);
        if (sf == null)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' not found");

        if (expectedStatuses.All(expectedStatus => expectedStatus != sf.Status))
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' did not have expected status: '{sf.Status}'");

        if (expectedEpoch != null && sf.Epoch != expectedEpoch)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' did not have expected epoch: '{sf.Epoch}'");

        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            var epoch = sf.Epoch + 1;
            var success = await _functionStore.TryToBecomeLeader(
                functionId,
                Status.Executing,
                expectedEpoch: sf.Epoch,
                newEpoch: epoch
            );

            if (!success)
                throw new UnexpectedFunctionState(functionId, $"Unable to become leader for function: '{functionId}'"); //todo concurrent modification exception

            var param = (TParam) _serializer.DeserializeParameter(sf.Parameter.ParamJson, sf.Parameter.ParamType);
            if (!hasScrapbook)
                return new PreparedReInvocation<TParam, RScrapbook>(param, epoch, default(RScrapbook), runningFunction);

            var scrapbook = _serializer.DeserializeScrapbook(
                sf.Scrapbook!.ScrapbookJson,
                sf.Scrapbook.ScrapbookType
            );
            scrapbook.Initialize(functionId, _functionStore, _serializer, epoch);

            return new PreparedReInvocation<TParam, RScrapbook>(param, epoch, (RScrapbook?) scrapbook, runningFunction);
        }
        catch (Exception)
        {
            runningFunction.Dispose();
            throw;
        }
    }

    internal record PreparedReInvocation<TParam>(TParam Param, int Epoch, IDisposable RunningFunction);
    internal record PreparedReInvocation<TParam, TScrapbook>(TParam Param, int Epoch, TScrapbook? Scrapbook, IDisposable RunningFunction)
        where TScrapbook : RScrapbook;

    public IDisposable StartSignOfLife(FunctionId functionId, int epoch = 0) 
        => _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
    public IDisposable RegisterRunningFunction() => _shutdownCoordinator.RegisterRunningRFunc();
}