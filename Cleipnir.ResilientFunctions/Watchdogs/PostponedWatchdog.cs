using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;

namespace Cleipnir.ResilientFunctions.Watchdogs;

internal class PostponedWatchdog : IDisposable
{
    private readonly IFunctionStore _functionStore;
    private readonly WatchdogFuncInvoker _watchdogFuncInvoker;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
        
    private readonly WatchdogFunc _watchdogFunc;

    private readonly TimeSpan _checkFrequency;
    private readonly FunctionTypeId _functionTypeId;
    private volatile bool _disposed;
    private volatile bool _executing;

    public PostponedWatchdog(
        FunctionTypeId functionTypeId, 
        WatchdogFunc watchdogFunc,
        IFunctionStore functionStore, 
        WatchdogFuncInvoker watchdogFuncInvoker,
        TimeSpan checkFrequency,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator
    )
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _watchdogFuncInvoker = watchdogFuncInvoker;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _watchdogFunc = watchdogFunc;
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
                    if (storedFunction?.Status != Status.Postponed || expired.Epoch != storedFunction.Epoch) continue;

                    try
                    {
                        await _watchdogFuncInvoker.ReInvoke(functionId, storedFunction, _watchdogFunc);
                    }
                    catch (Exception innerException)
                    {
                        _unhandledExceptionHandler.Invoke(
                            new FrameworkException(
                                _functionTypeId,
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
                    _functionTypeId,
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