using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class CrashedWatchdog
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly WatchDogReInvokeFunc _reInvoke;
    private readonly IFunctionStore _functionStore;
    private readonly TimeSpan _signOfLifeFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    
    private readonly AsyncSemaphore _asyncSemaphore;
    private readonly HashSet<FunctionInstanceId> _toBeExecuted = new();
    private readonly object _sync = new();

    public CrashedWatchdog(
        FunctionTypeId functionTypeId,
        IFunctionStore functionStore,
        WatchDogReInvokeFunc reInvoke,
        AsyncSemaphore asyncSemaphore,
        TimeSpan signOfLifeFrequency,
        TimeSpan delayStartUp,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _reInvoke = reInvoke;
        _asyncSemaphore = asyncSemaphore;
        _signOfLifeFrequency = signOfLifeFrequency;
        _delayStartUp = delayStartUp;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task Start()
    {
        if (_signOfLifeFrequency == TimeSpan.Zero) return;
        await Task.Delay(_delayStartUp);
        
        try
        {
            var prevFunctionCounts = new Dictionary<FunctionInstanceId, ObservationAndRemainingCount>();

            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                await Task.Delay(_signOfLifeFrequency);
                if (_shutdownCoordinator.ShutdownInitiated) return;

                var currExecutingFunctions = await _functionStore
                    .GetExecutingFunctions(_functionTypeId);

                var hangingFunctions = new List<StoredExecutingFunction>();
                var newFunctionCounts = new Dictionary<FunctionInstanceId, ObservationAndRemainingCount>();
                foreach (var sef in currExecutingFunctions)
                {
                    if (!prevFunctionCounts.ContainsKey(sef.InstanceId))
                        newFunctionCounts[sef.InstanceId] = new ObservationAndRemainingCount(sef.Epoch, sef.LastSignOfLife, CalculateRemainingCount(sef));
                    else
                    {
                        var prev = prevFunctionCounts[sef.InstanceId];
                        if (sef.LastSignOfLife != prev.SignOfLife || sef.Epoch != prev.Epoch)
                            newFunctionCounts[sef.InstanceId] = new ObservationAndRemainingCount(sef.Epoch, sef.LastSignOfLife, CalculateRemainingCount(sef));
                        else if (prevFunctionCounts[sef.InstanceId].RemainingCount == 0)
                            hangingFunctions.Add(sef);
                        else
                            newFunctionCounts[sef.InstanceId] = prevFunctionCounts[sef.InstanceId]
                                with
                                {
                                    RemainingCount = prev.RemainingCount - 1
                                };
                    }
                }

                foreach (var hangingFunction in hangingFunctions.RandomlyPermute())
                    _ = ReInvokeCrashedFunction(hangingFunction);

                prevFunctionCounts = newFunctionCounts;
            }
        }
        catch (Exception thrownException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(CrashedWatchdog)} for '{_functionTypeId}' failed",
                    innerException: thrownException
                )
            );
        }
    }

    private async Task ReInvokeCrashedFunction(StoredExecutingFunction sef)
    {
        lock (_sync)
            if (_toBeExecuted.Contains(sef.InstanceId))
                return;
            else
                _toBeExecuted.Add(sef.InstanceId);
        
        using var @lock = await _asyncSemaphore.Take();
        
        if (_shutdownCoordinator.ShutdownInitiated) return;
        try
        {
            await _reInvoke(sef.InstanceId, expectedEpoch: sef.Epoch, expectedStatus: Status.Executing);
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

    private int CalculateRemainingCount(StoredExecutingFunction sef) =>
        Math.Max(
            1,
            sef.SignOfLifeFrequency / _signOfLifeFrequency.Ticks
            + sef.SignOfLifeFrequency % _signOfLifeFrequency.Ticks == 0
                ? 0
                : 1
        ) - 1;

    private record ObservationAndRemainingCount(int Epoch, long SignOfLife, int RemainingCount);
}