using System;
using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class CrashedOrPostponedWatchdog
{
    private readonly IFunctionStore _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    
    private readonly LeasesUpdater _leasesUpdater;
    
    private volatile ImmutableDictionary<StoredType, Tuple<RestartFunction, ScheduleRestartFromWatchdog, AsyncSemaphore>> _flowsDictionary
        = ImmutableDictionary<StoredType, Tuple<RestartFunction, ScheduleRestartFromWatchdog, AsyncSemaphore>>.Empty;
    private readonly Lock _sync = new();
    private bool _started;

    public CrashedOrPostponedWatchdog(
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler, 
        TimeSpan checkFrequency, TimeSpan delayStartUp,
        LeasesUpdater leasesUpdater
    )
    {
        _functionStore = functionStore;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _leasesUpdater = leasesUpdater;
    }

    public void Register(
        StoredType storedType, 
        RestartFunction restartFunction, 
        ScheduleRestartFromWatchdog scheduleRestart,
        AsyncSemaphore asyncSemaphore)
    {
        _flowsDictionary = _flowsDictionary.SetItem(storedType, Tuple.Create(restartFunction, scheduleRestart, asyncSemaphore));
        
        lock (_sync)
        {
            if (!_started)
                Task.Run(Start);    
            
            _started = true;
        }
    }

    private async Task Start()
    {
        await Task.Delay(_delayStartUp);

        Start:
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var now = DateTime.UtcNow;

                var eligibleFunctions = await _functionStore.GetExpiredFunctions(expiresBefore: now.Ticks);
                eligibleFunctions = _leasesUpdater.FilterOutContains(eligibleFunctions);

                var flowsDictionary = _flowsDictionary;     
                foreach (var sef in eligibleFunctions.WithRandomOffset())
                {
                    if (!flowsDictionary.TryGetValue(sef.FlowId.Type, out var tuple))
                        continue;
                    
                    var (restartFunction, scheduleRestart, asyncSemaphore) = tuple;
                    if (!asyncSemaphore.TryTake(out var takenLock))
                        continue;
                    
                    var runningFunction = _shutdownCoordinator.TryRegisterRunningFunction();
                    if (runningFunction == null)
                    {
                        takenLock.Dispose();
                        return;
                    }

                    try
                    {
                        var restartedFunction = await restartFunction(sef.FlowId, sef.Epoch);
                        if (restartedFunction == null)
                        {
                            runningFunction.Dispose();
                            takenLock.Dispose();
                            break;
                        }

                        await scheduleRestart(
                            sef.FlowId.Instance,
                            restartedFunction,
                            onCompletion: () =>
                            {
                                takenLock.Dispose();
                                runningFunction.Dispose();
                            }
                        );
                    }
                    catch
                    {
                        runningFunction.Dispose();
                        takenLock.Dispose();
                        throw;
                    }
                }
                
                var timeElapsed = DateTime.UtcNow - now;
                var delay = (_checkFrequency - timeElapsed).RoundUpToZero();
                
                await Task.Delay(delay);
            }
        }
        catch (Exception thrownException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    $"{nameof(CrashedOrPostponedWatchdog)} execution failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }
}