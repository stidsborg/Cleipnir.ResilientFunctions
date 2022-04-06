using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Helpers.Disposables;
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
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value)
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
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value)
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
    
    public async Task PersistPostInvoked(
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
                    result.Postpone!.InProcessWait ? Status.Executing : Status.Postponed,
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

    public async Task PersistPostInvoked<TReturn>(
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
                    result.Postpone!.InProcessWait ? Status.Executing : Status.Postponed,
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

    public static void EnsureSuccess(FunctionId functionId, Result result)
    {
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return;
            case Outcome.Postpone:
                throw new FunctionInvocationPostponedException(functionId, result.Postpone!.DateTime);
            case Outcome.Fail:
                throw result.Fail!;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private static void EnsureSuccess<TReturn>(FunctionId functionId, Result<TReturn> result)
    {
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return;
            case Outcome.Postpone:
                throw new FunctionInvocationPostponedException(functionId, result.Postpone!.DateTime);
            case Outcome.Fail:
                throw result.Fail!;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public async Task<Tuple<TParam, TScrapbook, int>> PrepareForReInvocation<TParam, TScrapbook>(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        var (param, epoch, scrapbook) = await PrepareForReInvocation<TParam>(
            functionId,
            expectedStatuses,
            expectedEpoch,
            hasScrapbook: true
        );
        return Tuple.Create(
            param, 
            (TScrapbook) scrapbook!,
            epoch
        );
    }

    public async Task<Tuple<TParam, int>> PrepareForReInvocation<TParam>(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch)
        where TParam : notnull
    {
        var (param, epoch, _) = await PrepareForReInvocation<TParam>(
            functionId,
            expectedStatuses,
            expectedEpoch,
            hasScrapbook: false
        );
        return Tuple.Create(param, epoch);
    }
    
    private async Task<Tuple<TParam, int, RScrapbook?>> PrepareForReInvocation<TParam>(
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
            return Tuple.Create(param, epoch, default(RScrapbook));
        
        var scrapbook = _serializer.DeserializeScrapbook(
            sf.Scrapbook!.ScrapbookJson,
            sf.Scrapbook.ScrapbookType
        );
        scrapbook.Initialize(functionId, _functionStore, _serializer, epoch);
        
        return Tuple.Create(param, epoch, (RScrapbook?) scrapbook);
    }

    private async Task<bool> WaitForInProcessDelay(Result result)
    {
        if (result.Postpone?.InProcessWait != true) return false;

        _shutdownCoordinator.RegisterRFuncCompletion();
        var delay = CalculateDelay(result.Postpone);
        await Task.Delay(delay);
        _shutdownCoordinator.RegisterRunningRFunc();
        return true;
    }

    private async Task<bool> WaitForInProcessDelay<TReturn>(Result<TReturn> result)
    {
        if (result.Postpone?.InProcessWait != true) return false;

        _shutdownCoordinator.RegisterRFuncCompletion();
        var delay = CalculateDelay(result.Postpone);
        await Task.Delay(delay);
        _shutdownCoordinator.RegisterRunningRFunc();
        return true;
    }

    public static TimeSpan CalculateDelay(Postpone postpone)
    {
        var delay = postpone.DateTime - DateTime.UtcNow;
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;

        return delay;
    }

    public IDisposable CreateSignOfLifeAndRegisterRunningFunction(FunctionId functionId, int epoch = 0)
    {
        var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, epoch);
        var runningFunction = _shutdownCoordinator.RegisterRunningRFuncDisposable();
        return new CombinedDisposables(signOfLifeUpdater, runningFunction);
    }

    public async Task<InProcessWait> PersistResultAndEnsureSuccess<TReturn>(
        FunctionId functionId, 
        Result<TReturn> result, 
        RScrapbook? scrapbook, 
        int expectedEpoch)
    {
        await PersistPostInvoked(functionId, result, scrapbook: scrapbook, expectedEpoch);
        if (await WaitForInProcessDelay(result)) return InProcessWait.RetryInvocation;
        EnsureSuccess(functionId, result);
        return InProcessWait.DoNotRetryInvocation;
    }
    
    public async Task<InProcessWait> PersistResultAndEnsureSuccess(
        FunctionId functionId, 
        Result result, 
        RScrapbook? scrapbook, 
        int expectedEpoch)
    {
        await PersistPostInvoked(functionId, result, scrapbook: scrapbook, expectedEpoch);
        if (await WaitForInProcessDelay(result)) return InProcessWait.RetryInvocation;
        EnsureSuccess(functionId, result);
        return InProcessWait.DoNotRetryInvocation;
    }
}