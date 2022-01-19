using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation
{
    internal class RActionInvoker
    {
        private readonly IFunctionStore _functionStore;
        private readonly ISignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
        private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
        private readonly ShutdownCoordinator _shutdownCoordinator;

        public RActionInvoker(
            IFunctionStore functionStore, 
            ISignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
            UnhandledExceptionHandler unhandledExceptionHandler, 
            ShutdownCoordinator shutdownCoordinator
        )
        {
            _functionStore = functionStore;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _unhandledExceptionHandler = unhandledExceptionHandler;
            _shutdownCoordinator = shutdownCoordinator;
        }

        public async Task ReInvoke(
            FunctionId functionId,
            StoredFunction storedFunction,
            RAction rAction
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

                var parameter = storedFunction.Parameter.Deserialize();

                RScrapbook? scrapbook = null;
                if (storedFunction.Scrapbook != null)
                {
                    scrapbook = storedFunction.Scrapbook.Deserialize();
                    scrapbook.Initialize(functionId, _functionStore, newEpoch);
                }

                RResult result;
                try
                {
                    result = await rAction(parameter, scrapbook);
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
                        scrapbookJson: scrapbook?.ToJson(),
                        result: null,
                        failed: null,
                        postponedUntil: null,
                        newEpoch
                    ),
                    ResultType.Postponed => _functionStore.SetFunctionState(
                        functionId,
                        Status.Postponed,
                        scrapbookJson: scrapbook?.ToJson(),
                        result: null,
                        failed: null,
                        postponedUntil: result.PostponedUntil!.Value.Ticks,
                        newEpoch
                    ),
                    ResultType.Failed => _functionStore.SetFunctionState(
                        functionId,
                        Status.Failed,
                        scrapbookJson: scrapbook?.ToJson(),
                        result: null,
                        new StoredFailure(
                            FailedJson: result.FailedException!.ToJson(),
                            FailedType: result.FailedException!.GetType().SimpleQualifiedName()
                        ),
                        postponedUntil: null,
                        newEpoch
                    ),
                    _ => throw new ArgumentOutOfRangeException()
                };

                await setFunctionStateTask;

                if (result.FailedException != null)
                    _unhandledExceptionHandler.Invoke(new FunctionInvocationException(
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
}