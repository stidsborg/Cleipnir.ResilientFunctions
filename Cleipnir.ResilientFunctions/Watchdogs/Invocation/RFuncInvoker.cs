using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation;

internal class RFuncInvoker
{
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly ISignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    public RFuncInvoker(
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

    public async Task ReInvoke<TReturn>(
        FunctionId functionId,
        StoredFunction storedFunction,
        RFunc<TReturn> rFunc
    )
    {
        try
        {
            _shutdownCoordinator.RegisterRunningRFunc();
            var expectedEpoch = storedFunction.Epoch;
            var newEpoch = expectedEpoch + 1;
            var success = await _functionStore.TryToBecomeLeader(
                functionId,
                Status.Executing,
                expectedEpoch: expectedEpoch,
                newEpoch: newEpoch
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

            RResult<TReturn> result;
            try
            {
                result = await rFunc(parameter, scrapbook);
            }
            catch (Exception exception)
            {
                result = Fail.WithException(exception);
            }

            var setFunctionStateTask = result.ResultType switch
            {
                ResultType.Succeeded => _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson: scrapbook == null 
                        ? null 
                        : _serializer.SerializeScrapbook(scrapbook),
                    new StoredResult(
                        ResultJson: result.SuccessResult == null 
                            ? null 
                            : _serializer.SerializeResult(result.SuccessResult!),
                        ResultType: result.SuccessResult?.GetType().SimpleQualifiedName()
                    ),
                    failed: null,
                    postponedUntil: null,
                    newEpoch
                ),
                ResultType.Postponed => _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson: null,
                    result: null,
                    failed: null,
                    postponedUntil: result.PostponedUntil!.Value.Ticks,
                    newEpoch
                ),
                ResultType.Failed => _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson: null,
                    result: null,
                    new StoredFailure(
                        FailedJson: _serializer.SerializeFault(result.FailedException!),
                        FailedType: result.FailedException!.GetType().SimpleQualifiedName()
                    ),
                    postponedUntil: null,
                    newEpoch
                ),
                _ => throw new ArgumentOutOfRangeException()
            };

            await setFunctionStateTask;

            if (result.FailedException != null)
                _unhandledExceptionHandler.Invoke(new FunctionInvocationUnhandledException(
                    $"Function {functionId} threw unhandled exception",
                    result.FailedException
                ));
        }
        finally
        {
            _shutdownCoordinator.RegisterRFuncCompletion();
        }
    }
}