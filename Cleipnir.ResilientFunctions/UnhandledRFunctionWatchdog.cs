using System;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions
{
    internal class UnhandledRFunctionWatchdog<TParam, TReturn> : IDisposable 
    {
        private readonly FunctionTypeId _functionTypeId;
        private readonly Func<TParam, Task<TReturn>> _func;
        
        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

        private readonly TimeSpan _checkFrequency;
        private readonly Action<RFunctionException> _unhandledExceptionHandler;
        
        private volatile bool _disposed;

        public UnhandledRFunctionWatchdog(
            FunctionTypeId functionTypeId, 
            Func<TParam, Task<TReturn>> func, 
            IFunctionStore functionStore, 
            SignOfLifeUpdaterFactory signOfLifeUpdaterFactory, 
            TimeSpan checkFrequency, 
            Action<RFunctionException> unhandledExceptionHandler)
        {
            _functionTypeId = functionTypeId;
            _functionStore = functionStore;
            _signOfLifeUpdaterFactory = signOfLifeUpdaterFactory;
            _func = func;
            _checkFrequency = checkFrequency;
            _unhandledExceptionHandler = unhandledExceptionHandler;
        }

        public async Task Start()
        {
            if (_checkFrequency == TimeSpan.Zero) return;
            await Task.Yield();

            while (!_disposed)
                try
                {
                    await Task.Delay(_checkFrequency / 2);

                    if (_disposed) return;

                    var hangingFunctions = await _functionStore
                        .GetNonCompletedFunctions(_functionTypeId, DateTime.UtcNow.Ticks)
                        .RandomlyPermutate();

                    await Task.Delay(_checkFrequency / 2);

                    if (_disposed) return;

                    foreach (var function in hangingFunctions)
                        await RetryMethodInvocation(function);
                }
                catch (Exception e)
                {
                    _unhandledExceptionHandler(new FrameworkException(
                        $"UnhandledRFunctionWatchdog failed while executing: '{_functionTypeId}'", 
                        e)
                    );
                    return;
                }
        }

        private async Task RetryMethodInvocation(StoredFunction storedFunction)
        {
            var success = await _functionStore.UpdateSignOfLife(
                storedFunction.FunctionId, 
                storedFunction.SignOfLife, 
                DateTime.UtcNow.Ticks
            );
            
            if (!success)
                return;

            using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(
                storedFunction.FunctionId,
                storedFunction.SignOfLife
            );

            bool functionExecutionCompletedSuccessfully = false, functionExecutionStarted = false;
            try
            {
                var paramType = Type.GetType(storedFunction.ParamType, true)!;
                var param = (TParam) JsonSerializer.Deserialize(storedFunction.ParamJson, paramType)!;
                functionExecutionStarted = true;
                var result = await _func(param);
                var resultJson = JsonSerializer.Serialize(result);
                var resultType = result!.GetType().SimpleQualifiedName();
                functionExecutionCompletedSuccessfully = true;
                await _functionStore.StoreFunctionResult(storedFunction.FunctionId, resultJson, resultType);
            }
            catch (Exception e)
            {
                if (functionExecutionStarted && !functionExecutionCompletedSuccessfully)
                    _unhandledExceptionHandler(
                        new RFunctionInvocationException(
                            $"RFunction '{storedFunction.FunctionId}' invocation threw exception in UnhandledFunctionWatchdog",
                            e
                        )
                    );
                else
                    throw;
            }
        }

        public void Dispose() => _disposed = true;
    }
}