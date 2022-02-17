using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;
using _RAction = Cleipnir.ResilientFunctions.Watchdogs.Invocation.RAction;

namespace Cleipnir.ResilientFunctions.Watchdogs
{
    internal class CrashedWatchdog<TReturn> : IDisposable where TReturn : notnull
    {
        private readonly FunctionTypeId _functionTypeId;
        private readonly RFunc<TReturn> _func;

        private readonly IFunctionStore _functionStore;

        private readonly TimeSpan _checkFrequency;
        private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
        private readonly RFuncInvoker _rFuncInvoker;
        
        private volatile bool _disposed;
        private volatile bool _executing;

        public CrashedWatchdog(
            FunctionTypeId functionTypeId,
            RFunc<TReturn> func,
            IFunctionStore functionStore,
            RFuncInvoker rFuncInvoker,
            TimeSpan checkFrequency,
            UnhandledExceptionHandler unhandledExceptionHandler,
            ShutdownCoordinator shutdownCoordinator
        )
        {
            _functionTypeId = functionTypeId;
            _functionStore = functionStore;
            _func = func;
            _checkFrequency = checkFrequency;
            _unhandledExceptionHandler = unhandledExceptionHandler;
            _rFuncInvoker = rFuncInvoker;
            _disposed = !shutdownCoordinator.ObserveShutdown(DisposeAsync);
        }

        public async Task Start()
        {
            if (_checkFrequency == TimeSpan.Zero) return;
            try
            {
                var prevExecutingFunctions = new Dictionary<FunctionInstanceId, StoredFunctionStatus>();

                while (!_disposed)
                {
                    _executing = false;
                    await Task.Delay(_checkFrequency);
                    if (_disposed) return;
                    _executing = true;

                    var currExecutingFunctions = await _functionStore
                        .GetFunctionsWithStatus(_functionTypeId, Status.Executing)
                        .TaskSelect(l =>
                            l.ToDictionary(
                                s => s.InstanceId,
                                s => s
                            )
                        );

                    var hangingFunctions =
                        from prev in prevExecutingFunctions
                        join curr in currExecutingFunctions
                            on (prev.Key, prev.Value.SignOfLife) equals (curr.Key, curr.Value.SignOfLife)
                        select prev.Value;

                    foreach (var function in hangingFunctions.RandomlyPermutate())
                    {
                        if (_disposed) return;
                        
                        var functionId = new FunctionId(_functionTypeId, function.InstanceId);
                        var storedFunction = await _functionStore.GetFunction(functionId);
                        if (storedFunction == null)
                            throw new FrameworkException($"Function '{functionId}' not found on retry");
                        if (storedFunction.Status != Status.Executing) return;

                        try
                        {
                            await _rFuncInvoker.ReInvoke(functionId, storedFunction, _func);
                        }
                        catch (Exception innerException)
                        {
                            _unhandledExceptionHandler.Invoke(
                                new FrameworkException(
                                    $"{nameof(CrashedWatchdog<TReturn>)} failed while executing: '{functionId}'",
                                    innerException
                                )
                            );
                        }
                    }

                    prevExecutingFunctions = currExecutingFunctions;
                }
            }
            catch (Exception innerException)
            {
                _unhandledExceptionHandler.Invoke(
                    new FrameworkException(
                        $"{nameof(CrashedWatchdog<TReturn>)} failed while executing: '{_functionTypeId}'",
                        innerException
                    )
                );
            }
            finally
            {
                _executing = false;
            }
        }

        private async Task DisposeAsync()
        {
            _disposed = true;
            await BusyWait.ForeverUntilAsync(() => !_executing);
        }

        public void Dispose() => DisposeAsync().Wait();
    }
    
    internal class CrashedWatchdog : IDisposable 
    {
        private readonly FunctionTypeId _functionTypeId;
        private readonly _RAction _action;

        private readonly IFunctionStore _functionStore;

        private readonly TimeSpan _checkFrequency;
        private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
        private readonly RActionInvoker _rActionInvoker;

        private volatile bool _disposed;
        private volatile bool _executing;

        public CrashedWatchdog(
            FunctionTypeId functionTypeId,
            _RAction action,
            IFunctionStore functionStore,
            RActionInvoker rActionInvoker,
            TimeSpan checkFrequency,
            UnhandledExceptionHandler unhandledExceptionHandler,
            ShutdownCoordinator shutdownCoordinator
        )
        {
            _functionTypeId = functionTypeId;
            _functionStore = functionStore;
            _action = action;
            _checkFrequency = checkFrequency;
            _unhandledExceptionHandler = unhandledExceptionHandler;
            _rActionInvoker = rActionInvoker;
            _disposed = !shutdownCoordinator.ObserveShutdown(ShutdownGracefully);
        }

        public async Task Start()
        {
            if (_checkFrequency == TimeSpan.Zero) return;

            try
            {
                var prevHangingFunctions = new Dictionary<FunctionInstanceId, StoredFunctionStatus>();

                while (!_disposed)
                {
                    _executing = false;
                    await Task.Delay(_checkFrequency);
                    if (_disposed) return;
                    _executing = true;

                    var currHangingFunctions = await _functionStore
                        .GetFunctionsWithStatus(_functionTypeId, Status.Executing)
                        .TaskSelect(l =>
                            l.ToDictionary(
                                s => s.InstanceId,
                                s => s
                            )
                        );

                    var hangingFunctions =
                        from prev in prevHangingFunctions
                        join curr in currHangingFunctions
                            on (prev.Key, prev.Value.SignOfLife) equals (curr.Key, curr.Value.SignOfLife)
                        select prev.Value;

                    foreach (var function in hangingFunctions.RandomlyPermutate())
                    {
                        if (_disposed) return;
                        
                        var functionId = new FunctionId(_functionTypeId, function.InstanceId);
                        var storedFunction = await _functionStore.GetFunction(functionId);
                        if (storedFunction == null)
                            throw new FrameworkException($"Function '{functionId}' not found on retry");
                        if (storedFunction.Status != Status.Executing) return;

                        try
                        {
                            await _rActionInvoker.ReInvoke(functionId, storedFunction, _action);
                        }
                        catch (Exception innerException)
                        {
                            _unhandledExceptionHandler.Invoke(
                                new FrameworkException(
                                    $"{nameof(CrashedWatchdog)} failed while executing: '{functionId}'",
                                    innerException
                                )
                            );
                        }
                    }

                    prevHangingFunctions = currHangingFunctions;
                }
            }
            catch (Exception innerException)
            {
                _unhandledExceptionHandler.Invoke(
                    new FrameworkException(
                        $"{nameof(CrashedWatchdog)} failed while executing: '{_functionTypeId}'",
                        innerException
                    )
                );
            }
            finally
            {
                _executing = false;
            }
        }

        private Task ShutdownGracefully()
        {
            _disposed = true;
            return BusyWait.ForeverUntilAsync(() => !_executing);            
        }

        public void Dispose() => ShutdownGracefully().Wait();
    }
}