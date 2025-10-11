using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class PostponedWatchdog
{
    private readonly IFunctionStore _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly ClusterInfo _clusterInfo;
    
    private volatile ImmutableDictionary<StoredType, Tuple<RestartFunction, ScheduleRestartFromWatchdog, AsyncSemaphore>> _flowsDictionary
        = ImmutableDictionary<StoredType, Tuple<RestartFunction, ScheduleRestartFromWatchdog, AsyncSemaphore>>.Empty;
    private readonly Lock _sync = new();
    private bool _started;
    
    private readonly UtcNow _utcNow;

    public PostponedWatchdog(
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler, 
        TimeSpan checkFrequency, TimeSpan delayStartUp,
        ClusterInfo clusterInfo,
        UtcNow utcNow)
    {
        _functionStore = functionStore;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _clusterInfo = clusterInfo;
        _utcNow = utcNow;
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
                var now = _utcNow();

                var eligibleFunctions = await _functionStore.GetExpiredFunctions(expiresBefore: now.Ticks);
                
                var flowsDictionary = _flowsDictionary;     
                foreach (var id in eligibleFunctions.Where(s => s.AsULong % _clusterInfo.ReplicaCount == _clusterInfo.Offset))
                {
                    if (!flowsDictionary.TryGetValue(id.Type, out var tuple))
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
                        var restartedFunction = await restartFunction(id);
                        if (restartedFunction == null)
                        {
                            runningFunction.Dispose();
                            takenLock.Dispose();
                            break;
                        }

                        await scheduleRestart(
                            id,
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
                
                var timeElapsed = _utcNow() - now;
                var delay = (_checkFrequency - timeElapsed).RoundUpToZero();
                
                await Task.Delay(delay);
            }
        }
        catch (Exception thrownException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    $"{nameof(PostponedWatchdog)} execution failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }
}