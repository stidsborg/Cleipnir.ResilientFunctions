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
    private readonly IFunctionStore _functionStore;
    private readonly SettingsWithDefaults _settings;
    private readonly int _version;
    
    private ISerializer Serializer { get; }

    public CommonInvoker(
        SettingsWithDefaults settings,
        int version,
        IFunctionStore functionStore, 
        ShutdownCoordinator shutdownCoordinator)
    {
        _settings = settings;
        _version = version;
        
        Serializer = new ErrorHandlingDecorator(settings.Serializer);
        _shutdownCoordinator = shutdownCoordinator;
        _functionStore = functionStore;
    }

    public async Task<Tuple<bool, IDisposable>> PersistFunctionInStore<TParam>(FunctionId functionId, TParam param, Type? scrapbookType)
        where TParam : notnull
    {
        ArgumentNullException.ThrowIfNull(param);
        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        var paramJson = Serializer.SerializeParameter(param);
        var paramType = param.GetType().SimpleQualifiedName();
        try
        {
            var created = await _functionStore.CreateFunction(
                functionId,
                param: new StoredParameter(paramJson, paramType),
                scrapbookType: scrapbookType?.SimpleQualifiedName(),
                crashedCheckFrequency: _settings.CrashedCheckFrequency.Ticks,
                _version
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
    
    public async Task<TReturn> WaitForFunctionResult<TReturn>(FunctionId functionId) 
    {
        while (true)
        {
            var storedFunction = await _functionStore.GetFunction(functionId);
            if (storedFunction == null)
                throw new FrameworkException(functionId.TypeId, $"Function {functionId} does not exist");

            if (storedFunction.Version > _version)
                throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' is at unsupported version: '{_version}'");
            
            switch (storedFunction.Status)
            {
                case Status.Executing:
                    await Task.Delay(100);
                    continue;
                case Status.Succeeded:
                    return storedFunction.Result!.Deserialize<TReturn>(Serializer)!;
                case Status.Failed:
                    var error = Serializer.DeserializeError(storedFunction.ErrorJson!);
                    throw new PreviousFunctionInvocationException(functionId, error);
                case Status.Postponed:
                    throw new FunctionInvocationPostponedException(
                        functionId,
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc)
                    );
                default:
                    throw new ArgumentOutOfRangeException(); 
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
                    var error = Serializer.DeserializeError(storedFunction.ErrorJson!);
                    throw new PreviousFunctionInvocationException(functionId, error);
                case Status.Postponed:
                    throw new FunctionInvocationPostponedException(
                        functionId,
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc)
                    );
                default:
                    throw new ArgumentOutOfRangeException(); 
            }
        }
    }
    
    public TScrapbook CreateScrapbook<TScrapbook>(FunctionId functionId, int expectedEpoch, Type? concreteScrapbookType) where TScrapbook : RScrapbook, new()
    {
        var scrapbook = (TScrapbook) (
            concreteScrapbookType == null 
                ? new TScrapbook()
                : Activator.CreateInstance(concreteScrapbookType)!
            );
        scrapbook.Initialize(functionId, _functionStore, Serializer, expectedEpoch);
        return scrapbook;
    }
    
    public async Task PersistFailure(FunctionId functionId, Exception exception, RScrapbook? scrapbook, int expectedEpoch)
    {
        var scrapbookJson = scrapbook == null
            ? null
            : Serializer.SerializeScrapbook(scrapbook);
        
        var success = await _functionStore.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson,
            result: null,
            errorJson: Serializer.SerializeError(exception.ToError()),
            postponedUntil: null,
            expectedEpoch
        );
        if (!success) 
            throw new ConcurrentModificationException(functionId);
    }
    
    public async Task PersistResult(
        FunctionId functionId,
        Result result,
        RScrapbook? scrapbook,
        int expectedEpoch)
    {
        var scrapbookJson = scrapbook == null
            ? null
            : Serializer.SerializeScrapbook(scrapbook);

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
                    errorJson: Serializer.SerializeError(result.Fail!.ToError()),
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
            : Serializer.SerializeScrapbook(scrapbook);
        
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
                            : Serializer.SerializeResult(result.SucceedWithValue),
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
                    errorJson: Serializer.SerializeError(result.Fail!.ToError()),
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
        int? expectedEpoch,
        Action<TScrapbook>? scrapbookUpdater
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
    {
        var (param, epoch, scrapbook, runningFunction) = await PrepareForReInvocation<TParam, TScrapbook>(
            functionId,
            expectedStatuses,
            expectedEpoch,
            hasScrapbook: true,
            scrapbookUpdater
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
        var (param, epoch, _, runningFunction) = await PrepareForReInvocation<TParam, UnitScrapbook>(
            functionId,
            expectedStatuses,
            expectedEpoch,
            hasScrapbook: false,
            scrapbookUpdater: null
        );
        return new PreparedReInvocation<TParam>(param, epoch, runningFunction);
    }
    
    private async Task<PreparedReInvocation<TParam, TScrapbook>> PrepareForReInvocation<TParam, TScrapbook>(
        FunctionId functionId, IEnumerable<Status> expectedStatuses, int? expectedEpoch, 
        bool hasScrapbook, Action<TScrapbook>? scrapbookUpdater
    ) where TParam : notnull where TScrapbook : RScrapbook 
    {
        expectedStatuses = expectedStatuses.ToList();
        var sf = await _functionStore.GetFunction(functionId);
        if (sf == null)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' not found");
        if (expectedStatuses.All(expectedStatus => expectedStatus != sf.Status))
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' did not have expected status: '{sf.Status}'");
        if (expectedEpoch != null && sf.Epoch != expectedEpoch)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' did not have expected epoch: '{sf.Epoch}'");
        if (sf.Version > _version)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' is at unsupported version: '{sf.Version}'");
        if (hasScrapbook && sf.Scrapbook == null)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' did not have a scrapbook as expected");

        var updatedScrapbookJsonOption = Option<string>.None;
        if (scrapbookUpdater != null)
        {
            var scrapbook = _settings.Serializer.DeserializeScrapbook<TScrapbook>(
                sf.Scrapbook!.ScrapbookJson,
                sf.Scrapbook.ScrapbookType
            );
            scrapbookUpdater(scrapbook);
            updatedScrapbookJsonOption = Option.Some(Serializer.SerializeScrapbook(scrapbook));
        }
        
        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        var epoch = sf.Epoch + 1;
        try
        {
            var success = await _functionStore.TryToBecomeLeader(
                functionId,
                Status.Executing,
                expectedEpoch: sf.Epoch,
                newEpoch: epoch,
                _settings.CrashedCheckFrequency.Ticks,
                _version,
                updatedScrapbookJsonOption
            );

            if (!success)
                throw new UnexpectedFunctionState(functionId, $"Unable to become leader for function: '{functionId}'");

            var param = Serializer.DeserializeParameter<TParam>(sf.Parameter.ParamJson, sf.Parameter.ParamType);
            if (!hasScrapbook)
                return new PreparedReInvocation<TParam, TScrapbook>(param, epoch, default(TScrapbook), runningFunction);

            var scrapbook = updatedScrapbookJsonOption.HasValue
                ? Serializer.DeserializeScrapbook<TScrapbook>(updatedScrapbookJsonOption.Value, sf.Scrapbook!.ScrapbookType)
                : Serializer.DeserializeScrapbook<TScrapbook>(sf.Scrapbook!.ScrapbookJson, sf.Scrapbook.ScrapbookType);
            scrapbook.Initialize(functionId, _functionStore, Serializer, epoch);
            
            return new PreparedReInvocation<TParam, TScrapbook>(param, epoch, (TScrapbook?) scrapbook, runningFunction);
        }
        catch (DeserializationException e)
        {
            await _functionStore.SetFunctionState(
                functionId,
                Status.Failed,
                sf.Scrapbook?.ScrapbookJson,
                sf.Result,
                Serializer.SerializeError(e.ToError()),
                postponedUntil: null,
                expectedEpoch: epoch
            );
            throw;
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
        => SignOfLifeUpdater.CreateAndStart(functionId, epoch, _functionStore, _settings);
    
    public IDisposable RegisterRunningFunction() => _shutdownCoordinator.RegisterRunningRFunc();
}