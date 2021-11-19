using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions
{
    internal class UnhandledRFunctionWatchdog<TReturn> : IDisposable where TReturn : notnull
    {
        public delegate Task<TReturn> RFunc(object param1, object? param2, RScrapbook? scrapbook);
        
        private readonly FunctionTypeId _functionTypeId;
        private readonly RFunc _func;
        
        private readonly IFunctionStore _functionStore;
        private readonly SignOfLifeUpdaterFactory _signOfLifeUpdaterFactory;

        private readonly TimeSpan _checkFrequency;
        private readonly Action<RFunctionException> _unhandledExceptionHandler;
        
        private volatile bool _disposed;

        public UnhandledRFunctionWatchdog(
            FunctionTypeId functionTypeId, 
            RFunc func, 
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

            await Task.Run(async () =>
            {
                try
                {
                    var prevHangingFunctions = new List<NonCompletedFunction>();

                    while (!_disposed)
                    {

                        await Task.Delay(_checkFrequency);

                        if (_disposed) return;

                        var currHangingFunctions = await _functionStore
                            .GetNonCompletedFunctions(_functionTypeId)
                            .ToTaskList();

                        var hangingFunctions =
                            from prev in prevHangingFunctions
                            join curr in currHangingFunctions on prev equals curr
                            select prev;

                        foreach (var function in hangingFunctions.RandomlyPermutate())
                            await RetryMethodInvocation(function);

                        prevHangingFunctions = currHangingFunctions;
                    }
                }
                catch (Exception e)
                {
                    _unhandledExceptionHandler(new FrameworkException(
                        $"UnhandledRFunctionWatchdog failed while executing: '{_functionTypeId}'",
                        e)
                    );
                }
            });
        }

        private async Task RetryMethodInvocation(NonCompletedFunction nonCompletedFunction)
        {
            var functionId = new FunctionId(_functionTypeId, nonCompletedFunction.InstanceId);
            var storedFunction = await _functionStore.GetFunction(functionId);
            if (storedFunction == null)
                throw new FrameworkException($"Function '{functionId}' not found on retry");
            
            var success = await _functionStore.UpdateSignOfLife(
                storedFunction.FunctionId, 
                nonCompletedFunction.LastSignOfLife, 
                DateTime.UtcNow.Ticks
            );
            
            if (!success || storedFunction.Result != null)
                return;

            using var signOfLifeUpdater = _signOfLifeUpdaterFactory.CreateAndStart(
                storedFunction.FunctionId,
                storedFunction.SignOfLife
            );

            bool functionExecutionCompletedSuccessfully = false, functionExecutionStarted = false;
            try
            {
                var parameter1 = JsonSerializer.Deserialize(
                    storedFunction.Parameter1.ParamJson,
                    Type.GetType(storedFunction.Parameter1.ParamType, throwOnError: true)!
                )!;

                object? parameter2 = null;
                if (storedFunction.Parameter2 != null)
                    parameter2 = JsonSerializer.Deserialize(
                        storedFunction.Parameter1.ParamJson,
                        Type.GetType(storedFunction.Parameter1.ParamType, throwOnError: true)!
                    )!;

                RScrapbook? scrapbook = null;
                if (storedFunction.Scrapbook != null)
                    scrapbook = (RScrapbook) JsonSerializer.Deserialize(
                        storedFunction.Scrapbook.ScrapbookJson!, //todo what if scrapbook json is null
                        Type.GetType(storedFunction.Scrapbook.ScrapbookType, true)!
                    )!;
                
                functionExecutionStarted = true;
                var result = await _func(parameter1, parameter2, scrapbook);
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