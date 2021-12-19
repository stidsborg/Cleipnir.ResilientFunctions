using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.SignOfLife;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions.Watchdogs.Invocation
{
    internal class RFuncInvoker
    {
        private readonly IFunctionStore _functionStore;
        private readonly ISignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;
        private readonly Action<RFunctionException> _unhandledExceptionHandler;

        public RFuncInvoker(
            IFunctionStore functionStore, 
            ISignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
            Action<RFunctionException> unhandledExceptionHandler
        )
        {
            _functionStore = functionStore;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _unhandledExceptionHandler = unhandledExceptionHandler;
        }

        public async Task ReInvoke<TReturn>(
            FunctionId functionId,
            StoredFunction storedFunction,
            RFunc<TReturn> rFunc
        )
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

            var parameter = storedFunction.Parameter.Deserialize();
            
            RScrapbook? scrapbook = null;
            if (storedFunction.Scrapbook != null)
            {
                scrapbook = storedFunction.Scrapbook.Deserialize();
                scrapbook.Initialize(functionId, _functionStore, newEpoch);
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
                    scrapbookJson: scrapbook?.ToJson(),
                    new StoredResult(
                        ResultJson: result.SuccessResult!.ToJson(),
                        ResultType: result.SuccessResult!.GetType().SimpleQualifiedName()
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
                _unhandledExceptionHandler(new FunctionInvocationException(
                    $"Function {functionId} threw unhandled exception",
                    result.FailedException
                ));
        }
        
        public async Task ReInvoke(
            FunctionId functionId,
            StoredFunction storedFunction,
            RAction rAction
        )
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
                _unhandledExceptionHandler(new FunctionInvocationException(
                    $"Function {functionId} threw unhandled exception",
                    result.FailedException
                ));
        }
    }
}