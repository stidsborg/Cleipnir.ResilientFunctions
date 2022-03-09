using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation;

internal class WrapperInnerFuncInvoker
{
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly ISignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    public WrapperInnerFuncInvoker(
        IFunctionStore functionStore, 
        ISerializer serializer,
        ISignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
        UnhandledExceptionHandler unhandledExceptionHandler, 
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionStore = functionStore;
        _serializer = serializer;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task ReInvoke(
        FunctionId functionId,
        StoredFunction storedFunction,
        WrappedInnerFunc wrappedInnerFunc
    )
    {
        _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            var expectedEpoch = storedFunction.Epoch;
            var newEpoch = expectedEpoch + 1;
            var success = await _functionStore.TryToBecomeLeader(
                functionId,
                Status.Executing,
                expectedEpoch,
                newEpoch
            );

            if (!success) return;

            using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(functionId, newEpoch);

            var parameter = storedFunction.Parameter.Deserialize(_serializer);

            RScrapbook? scrapbook = null;
            if (storedFunction.Scrapbook != null)
            {
                scrapbook = storedFunction.Scrapbook.Deserialize(_serializer);
                scrapbook.Initialize(functionId, _functionStore, _serializer, newEpoch);
            }

            Return<object?> returned;
            try
            {
                returned = await wrappedInnerFunc(parameter, scrapbook);
            }
            catch (Exception exception)
            {
                _unhandledExceptionHandler.Invoke(
                    new InnerFunctionUnhandledException(
                        functionId,
                        $"Function {functionId} threw unhandled exception",
                        exception
                    )
                );
                returned = Fail.WithException(exception);
            }

            var scrapbookJson = scrapbook == null
                ? null
                : _serializer.SerializeScrapbook(scrapbook);
            
            var setFunctionStateTask = returned.Intent switch
            {
                Intent.Succeed => _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson,
                    result: returned.SucceedWithValue == null 
                        ? null
                        : new StoredResult(
                            ResultJson: _serializer.SerializeResult(returned.SucceedWithValue!),
                            ResultType: returned.SucceedWithValue.GetType().SimpleQualifiedName()
                        ),
                    errorJson: null,
                    postponedUntil: null,
                    newEpoch
                ),
                Intent.Postpone => _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson,
                    result: null,
                    errorJson: null,
                    postponedUntil: returned.Postpone!.Value.Ticks,
                    newEpoch
                ),
                Intent.Fail => _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson,
                    result: null,
                    errorJson: _serializer.SerializeError(returned.Fail!.ToError()),
                    postponedUntil: null,
                    newEpoch
                ),
                _ => throw new ArgumentOutOfRangeException()
            };

            await setFunctionStateTask;
        }
        finally
        {
            _shutdownCoordinator.RegisterRFuncCompletion();
        }
    }
}