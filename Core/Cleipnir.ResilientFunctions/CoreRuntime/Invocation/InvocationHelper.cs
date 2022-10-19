using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal class InvocationHelper<TParam, TScrapbook, TReturn> 
    where TParam : notnull where TScrapbook : RScrapbook, new() 
{
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly IFunctionStore _functionStore;
    private readonly SettingsWithDefaults _settings;
    private readonly int _version;
    
    private ISerializer Serializer { get; }

    public InvocationHelper(
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

    public async Task<Tuple<bool, IDisposable>> PersistFunctionInStore(FunctionId functionId, TParam param, TScrapbook scrapbook)
    {
        ArgumentNullException.ThrowIfNull(param);
        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            var paramJson = Serializer.SerializeParameter(param);
            var paramType = param.GetType().SimpleQualifiedName();
            var scrapbookJson = Serializer.SerializeScrapbook(scrapbook);
            var scrapbookType = scrapbook.GetType().SimpleQualifiedName();
            var created = await _functionStore.CreateFunction(
                functionId,
                param: new StoredParameter(paramJson, paramType),
                storedScrapbook: new StoredScrapbook(scrapbookJson, scrapbookType),
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
    
    public async Task<TReturn> WaitForFunctionResult(FunctionId functionId) 
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
    
    public TScrapbook CreateScrapbook(FunctionId functionId, int expectedEpoch, Type? concreteScrapbookType)
    {
        var scrapbook = (TScrapbook) (
            concreteScrapbookType == null 
                ? new TScrapbook()
                : Activator.CreateInstance(concreteScrapbookType)!
            );
        scrapbook.Initialize(functionId, _functionStore, Serializer, expectedEpoch);
        return scrapbook;
    }

    public void InitializeScrapbook(FunctionId functionId, RScrapbook scrapbook, int epoch) 
        => scrapbook.Initialize(functionId, _functionStore, Serializer, epoch);
    
    public async Task PersistFailure(FunctionId functionId, Exception exception, RScrapbook scrapbook, int expectedEpoch)
    {
        var success = await _functionStore.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson: Serializer.SerializeScrapbook(scrapbook),
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
        Result<TReturn> result, 
        RScrapbook scrapbook,
        int expectedEpoch)
    {
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                var success = await _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson: Serializer.SerializeScrapbook(scrapbook),
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
                    scrapbookJson: Serializer.SerializeScrapbook(scrapbook),
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
                    scrapbookJson: Serializer.SerializeScrapbook(scrapbook),
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

    public static void EnsureSuccess(FunctionId functionId, Result<TReturn> result, bool allowPostponed)
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

    public async Task<PreparedReInvocation> PrepareForReInvocation(
        FunctionId functionId, 
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch,
        Action<TScrapbook>? scrapbookUpdater
    )
    {
        var (param, epoch, scrapbook, runningFunction) = await PrepareForReInvocation(
            functionId,
            expectedStatuses,
            expectedEpoch,
            hasScrapbook: true,
            scrapbookUpdater
        );
        return new PreparedReInvocation(
            param,
            epoch, 
            scrapbook,
            runningFunction
        );
    }

    private async Task<PreparedReInvocation> PrepareForReInvocation(
        FunctionId functionId, IEnumerable<Status> expectedStatuses, int? expectedEpoch, 
        bool hasScrapbook, Action<TScrapbook>? scrapbookUpdater
    ) 
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

        var updatedScrapbookJson = default(string);
        if (scrapbookUpdater != null)
        {
            var scrapbook = _settings.Serializer.DeserializeScrapbook<TScrapbook>(
                sf.Scrapbook.ScrapbookJson,
                sf.Scrapbook.ScrapbookType
            );
            scrapbookUpdater(scrapbook);
            updatedScrapbookJson = Serializer.SerializeScrapbook(scrapbook);
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
                _version
            );

            if (!success)
                throw new UnexpectedFunctionState(functionId, $"Unable to become leader for function: '{functionId}'");

            var param = Serializer.DeserializeParameter<TParam>(sf.Parameter.ParamJson, sf.Parameter.ParamType);
            
            var scrapbook = updatedScrapbookJson != null
                ? Serializer.DeserializeScrapbook<TScrapbook>(updatedScrapbookJson, sf.Scrapbook.ScrapbookType)
                : Serializer.DeserializeScrapbook<TScrapbook>(sf.Scrapbook.ScrapbookJson, sf.Scrapbook.ScrapbookType);
            scrapbook.Initialize(functionId, _functionStore, Serializer, epoch);
            
            return new PreparedReInvocation(param, epoch, scrapbook, runningFunction);
        }
        catch (DeserializationException e)
        {
            runningFunction.Dispose();
            await _functionStore.SetFunctionState(
                functionId,
                Status.Failed,
                sf.Scrapbook.ScrapbookJson,
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

    internal record PreparedReInvocation(TParam Param, int Epoch, TScrapbook Scrapbook, IDisposable RunningFunction);

    public IDisposable StartSignOfLife(FunctionId functionId, int epoch = 0) 
        => SignOfLifeUpdater.CreateAndStart(functionId, epoch, _functionStore, _settings);

    public async Task UpdateScrapbook(FunctionId functionId, Func<TScrapbook, TScrapbook> updater)
    {
        var serializer = _settings.Serializer;
        var sf = await _functionStore.GetFunction(functionId);
        
        if (sf == null)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' not found");
        if (sf.Status != Status.Failed)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' had status: '{sf.Status}' but must have failed");

        var scrapbook = serializer.DeserializeScrapbook<TScrapbook>(sf.Scrapbook.ScrapbookJson, sf.Scrapbook.ScrapbookType);
        var updatedScrapbook = updater(scrapbook);
        var updatedScrapbookJson = serializer.SerializeScrapbook(updatedScrapbook);
        var success = await _functionStore.SetParameters(
            functionId,
            storedParameter: null,
            storedScrapbook: new StoredScrapbook(
                updatedScrapbookJson, 
                ScrapbookType: updatedScrapbookJson.GetType().SimpleQualifiedName()
            ),
            expectedEpoch: sf.Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(functionId);
    }
    
    public async Task UpdateParameter(FunctionId functionId, Func<TParam, TParam> updater)
    {
        var serializer = _settings.Serializer;
        var sf = await _functionStore.GetFunction(functionId);
        
        if (sf == null)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' not found");
        if (sf.Status != Status.Failed)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' had status: '{sf.Status}' but must have failed");

        var parameter = serializer.DeserializeParameter<TParam>(sf.Parameter.ParamJson, sf.Parameter.ParamType);
        var updatedParam = updater(parameter);
        var updatedParamJson = serializer.SerializeParameter(updatedParam);
        var success = await _functionStore.SetParameters(
            functionId,
            storedParameter: null,
            storedScrapbook: new StoredScrapbook(
                updatedParamJson, 
                ScrapbookType: updatedParamJson.GetType().SimpleQualifiedName()
            ),
            expectedEpoch: sf.Epoch
        );
        
        if (!success)
            throw new ConcurrentModificationException(functionId);
    }
}