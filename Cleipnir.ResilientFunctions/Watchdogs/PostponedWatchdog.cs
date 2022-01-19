using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;

namespace Cleipnir.ResilientFunctions.Watchdogs
{
    internal class PostponedWatchdog<TReturn> : IDisposable
    {
        private readonly IFunctionStore _functionStore;
        private readonly RFuncInvoker _rFuncInvoker;
        private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
        
        private readonly RFunc<TReturn> _func;

        private readonly TimeSpan _checkFrequency;
        private readonly FunctionTypeId _functionTypeId;
        private volatile bool _disposed;
        private volatile bool _executing;

        public PostponedWatchdog(
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
            _rFuncInvoker = rFuncInvoker;
            _unhandledExceptionHandler = unhandledExceptionHandler;
            _func = func;
            _checkFrequency = checkFrequency;
            _disposed = !shutdownCoordinator.ObserveShutdown(DisposeAsync);
        }

        public async Task Start()
        {
            if (_checkFrequency == TimeSpan.Zero) return;

            try
            {
                while (!_disposed)
                {
                    _executing = false;
                    await Task.Delay(_checkFrequency);
                    if (_disposed) return;
                    _executing = true;

                    var expires = await _functionStore
                        .GetFunctionsWithStatus(_functionTypeId, Status.Postponed, DateTime.UtcNow.Ticks)
                        .RandomlyPermutate();

                    foreach (var expired in expires)
                    {
                        if (_disposed) return;

                        var functionId = new FunctionId(_functionTypeId, expired.InstanceId);
                        var storedFunction = await _functionStore.GetFunction(functionId);
                        if (storedFunction == null) continue;

                        try
                        {
                            await _rFuncInvoker.ReInvoke(functionId, storedFunction, _func);
                        }
                        catch (Exception innerException)
                        {
                            _unhandledExceptionHandler.Invoke(
                                new FrameworkException(
                                    $"{nameof(PostponedWatchdog<TReturn>)} failed while executing: '{functionId}'",
                                    innerException
                                )
                            );
                        }
                    }
                }
            }
            catch (Exception innerException)
            {
                _unhandledExceptionHandler.Invoke(
                    new FrameworkException(
                        $"{nameof(PostponedWatchdog<TReturn>)} failed while executing: '{_functionTypeId}'",
                        innerException
                    )
                );
            }
            finally
            {
                _executing = false;
            }
        }

        private Task DisposeAsync()
        {
            _disposed = true;
            return BusyWait.ForeverUntilAsync(() => !_executing);
        }

        public void Dispose() => DisposeAsync().Wait();
    }
    
    internal class PostponedWatchdog : IDisposable
    {
        private readonly IFunctionStore _functionStore;
        private readonly RActionInvoker _rActionInvoker;
        private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
        
        private readonly RAction _action;

        private readonly TimeSpan _checkFrequency;
        private readonly FunctionTypeId _functionTypeId;
        private volatile bool _disposed;
        private volatile bool _executing;

        public PostponedWatchdog(
            FunctionTypeId functionTypeId, 
            RAction action,
            IFunctionStore functionStore, 
            RActionInvoker rActionInvoker,
            TimeSpan checkFrequency,
            UnhandledExceptionHandler unhandledExceptionHandler,
            ShutdownCoordinator shutdownCoordinator
        )
        {
            _functionTypeId = functionTypeId;
            _functionStore = functionStore;
            _rActionInvoker = rActionInvoker;
            _unhandledExceptionHandler = unhandledExceptionHandler;
            _action = action;
            _checkFrequency = checkFrequency;
            _disposed = !shutdownCoordinator.ObserveShutdown(DisposeAsync);
        }

        public async Task Start()
        {
            if (_checkFrequency == TimeSpan.Zero) return;

            try
            {
                while (!_disposed)
                {
                    _executing = false;
                    await Task.Delay(_checkFrequency);
                    if (_disposed) return;
                    _executing = true;

                    var expires = await _functionStore
                        .GetFunctionsWithStatus(_functionTypeId, Status.Postponed, DateTime.UtcNow.Ticks)
                        .RandomlyPermutate();

                    foreach (var expired in expires)
                    {
                        if (_disposed) return;

                        var functionId = new FunctionId(_functionTypeId, expired.InstanceId);
                        var storedFunction = await _functionStore.GetFunction(functionId);
                        if (storedFunction == null) continue;

                        try
                        {
                            await _rActionInvoker.ReInvoke(functionId, storedFunction, _action);
                        }
                        catch (Exception innerException)
                        {
                            _unhandledExceptionHandler.Invoke(
                                new FrameworkException(
                                    $"{nameof(PostponedWatchdog)} failed while executing: '{functionId}'",
                                    innerException
                                )
                            );
                        }
                    }
                }
            }
            catch (Exception innerException)
            {
                _unhandledExceptionHandler.Invoke(
                    new FrameworkException(
                        $"{nameof(PostponedWatchdog)} failed while executing: '{_functionTypeId}'",
                        innerException
                    )
                );
            }
            finally
            {
                _executing = false;
            }
        }

        private Task DisposeAsync()
        {
            _disposed = true;
            return BusyWait.ForeverUntilAsync(() => !_executing);
        }

        public void Dispose() => DisposeAsync().Wait();
    }
}