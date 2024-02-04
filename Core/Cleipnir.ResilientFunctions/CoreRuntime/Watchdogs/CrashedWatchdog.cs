using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class CrashedWatchdog
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly ReInvoke _reInvoke;
    private readonly IFunctionStore _functionStore;
    private readonly TimeSpan _leaseLength;
    private readonly TimeSpan _delayStartUp;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    
    private readonly AsyncSemaphore _maxParallelismSemaphore;
    private readonly HashSet<FunctionInstanceId> _toBeExecuted = new();
    private readonly object _sync = new();

    public CrashedWatchdog(
        FunctionTypeId functionTypeId,
        IFunctionStore functionStore,
        ReInvoke reInvoke,
        AsyncSemaphore maxParallelismSemaphore,
        TimeSpan leaseLength,
        TimeSpan delayStartUp,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _reInvoke = reInvoke;
        _maxParallelismSemaphore = maxParallelismSemaphore;
        _leaseLength = leaseLength;
        _delayStartUp = delayStartUp;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task Start()
    {
        if (_leaseLength == TimeSpan.Zero) return;
        await Task.Delay(_delayStartUp);

        Start:
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var hangingFunctions = 
                    await _functionStore.GetCrashedFunctions(_functionTypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks);
                    
                foreach (var hangingFunction in hangingFunctions.RandomlyPermute())
                    _ = ReInvokeCrashedFunction(hangingFunction);
                
                await Task.Delay(_leaseLength);
            }
        }
        catch (Exception thrownException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(CrashedWatchdog)} for '{_functionTypeId}' failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }

    private async Task ReInvokeCrashedFunction(StoredExecutingFunction sef)
    {
        lock (_sync)
            if (!_toBeExecuted.Add(sef.InstanceId))
                return;

        using var @lock = await _maxParallelismSemaphore.Take();
        
        if (_shutdownCoordinator.ShutdownInitiated || sef.LeaseExpiration > DateTime.UtcNow.Ticks) return;
        try
        {
            await _reInvoke(sef.InstanceId.Value, expectedEpoch: sef.Epoch);
        }
        catch (ObjectDisposedException) { } //ignore when rfunctions has been disposed
        catch (UnexpectedFunctionState) { } //ignore when the functions state has changed since fetching it
        catch (FunctionInvocationPostponedException) { }
        catch (FunctionInvocationSuspendedException) { }
        catch (Exception innerException)
        {
            var functionId = new FunctionId(_functionTypeId, sef.InstanceId);
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(CrashedWatchdog)} failed while executing: '{functionId}'",
                    innerException
                )
            );
        }
        finally
        {
            lock (_sync)
                _toBeExecuted.Remove(sef.InstanceId);
        }
    }
}