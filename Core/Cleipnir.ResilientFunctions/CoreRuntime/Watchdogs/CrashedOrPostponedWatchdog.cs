using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
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
    

    private volatile ImmutableDictionary<FlowType, Tuple<RestartFunction, ScheduleRestartFromWatchdog, AsyncSemaphore>> _flowsDictionary
        = ImmutableDictionary<FlowType, Tuple<RestartFunction, ScheduleRestartFromWatchdog, AsyncSemaphore>>.Empty;
    private readonly object _sync = new();
    private bool _isStarted;

    public CrashedOrPostponedWatchdog(
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler, 
        TimeSpan checkFrequency, TimeSpan delayStartUp
    )
    {
        _functionStore = functionStore;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
    }

    public void Register(
        FlowType flowType, 
        RestartFunction restartFunction, 
        ScheduleRestartFromWatchdog scheduleRestart,
        AsyncSemaphore asyncSemaphore)
    {
        _flowsDictionary = _flowsDictionary.SetItem(flowType, Tuple.Create(restartFunction, scheduleRestart, asyncSemaphore));

        var isStarted = false;
        lock (_sync)
        {
            isStarted = _isStarted;
            _isStarted = true;
        }

        if (!isStarted)
            Task.Run(Start);
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
                #if DEBUG
                    eligibleFunctions = await ReAssertEligibleFunctions(eligibleFunctions, now);
                #endif

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

    private async Task<IReadOnlyList<IdAndEpoch>> ReAssertEligibleFunctions(IReadOnlyList<IdAndEpoch> eligibleFunctions, DateTime expiresBefore)
    {
        //race-condition fix between re-invoker and lease-updater. Task.Delays are not respected when debugging.
        //fix is to allow lease updater to update lease before crashed watchdog asserts that the functions in question has crashed
        
        if (eligibleFunctions.Count == 0 || !Debugger.IsAttached)
            return eligibleFunctions;
        
        await Task.Delay(500);
        var eligibleFunctionsRepeated = 
            (await _functionStore.GetExpiredFunctions(expiresBefore.Ticks)).ToHashSet();
        
        return eligibleFunctions.Where(ie => eligibleFunctionsRepeated.Contains(ie)).ToList();
    }
}