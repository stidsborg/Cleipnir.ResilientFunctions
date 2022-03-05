using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation;

internal class RActionInvoker
{
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly ISignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    public RActionInvoker(
        IFunctionStore functionStore,  
        ISerializer serializer,
        ISignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
        UnhandledExceptionHandler unhandledExceptionHandler, 
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionStore = functionStore;
        _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _serializer = serializer;
    }

    public async Task ReInvoke(
        FunctionId functionId,
        StoredFunction storedFunction,
        InnerAction innerAction
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

            Return returned;
            try
            {
                returned = await innerAction(parameter, scrapbook);
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

            var setFunctionStateTask = returned.Intent switch
            {
                Intent.Succeed => _functionStore.SetFunctionState(
                    functionId,
                    Status.Succeeded,
                    scrapbookJson: scrapbook == null 
                        ? null 
                        : _serializer.SerializeScrapbook(scrapbook),
                    result: null,
                    errorJson: null,
                    postponedUntil: null,
                    newEpoch
                ),
                Intent.Postpone => _functionStore.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson: scrapbook == null 
                        ? null 
                        : _serializer.SerializeScrapbook(scrapbook),
                    result: null,
                    errorJson: null,
                    postponedUntil: returned.Postpone!.Value.Ticks,
                    newEpoch
                ),
                Intent.Fail => _functionStore.SetFunctionState(
                    functionId,
                    Status.Failed,
                    scrapbookJson: scrapbook == null 
                        ? null 
                        : _serializer.SerializeScrapbook(scrapbook),
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