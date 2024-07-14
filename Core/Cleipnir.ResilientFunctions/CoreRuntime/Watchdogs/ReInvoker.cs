using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class ReInvoker
{
    public delegate Task<IReadOnlyList<InstanceIdAndEpoch>> GetEligibleFunctions(FlowType flowType, IFunctionStore functionStore, long t);
    
    private readonly FlowType _flowType;

    private readonly IFunctionStore _functionStore;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;

    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    
    private readonly AsyncSemaphore _asyncSemaphore;
    
    private readonly RestartFunction _restartFunction;
    private readonly ScheduleReInvokeFromWatchdog _scheduleReInvoke;
    private readonly GetEligibleFunctions _getEligibleFunctions;

    public ReInvoker(
        FlowType flowType, 
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler, 
        TimeSpan checkFrequency, TimeSpan delayStartUp, 
        AsyncSemaphore asyncSemaphore, 
        RestartFunction restartFunction, ScheduleReInvokeFromWatchdog scheduleReInvoke,
        GetEligibleFunctions getEligibleFunctions
    )
    {
        _flowType = flowType;
        _functionStore = functionStore;
        _shutdownCoordinator = shutdownCoordinator;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _asyncSemaphore = asyncSemaphore;
        _restartFunction = restartFunction;
        _scheduleReInvoke = scheduleReInvoke;
        _getEligibleFunctions = getEligibleFunctions;
    }
    
    public async Task Start(string watchdogName)
    {
        await Task.Delay(_delayStartUp);

        Start:
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var now = DateTime.UtcNow;

                var eligibleFunctions = await _getEligibleFunctions(_flowType, _functionStore, now.Ticks);
                #if DEBUG
                    eligibleFunctions = await ReAssertCrashedFunctions(eligibleFunctions, now);
                #endif
                
                foreach (var sef in eligibleFunctions.WithRandomOffset())
                {
                    if (!_asyncSemaphore.TryTake(out var takenLock))
                        break;
                    
                    var runningFunction = _shutdownCoordinator.TryRegisterRunningFunction();
                    if (runningFunction == null)
                    {
                        takenLock.Dispose();
                        return;
                    }
                    
                    var functionId = new FlowId(_flowType, sef.Instance);
                    var restartedFunction = await _restartFunction(functionId, sef.Epoch);
                    if (restartedFunction == null)
                    {
                        runningFunction.Dispose();
                        takenLock.Dispose();
                        break;
                    }

                    await _scheduleReInvoke(
                        sef.Instance,
                        restartedFunction,
                        onCompletion: () =>
                        {
                            takenLock.Dispose();
                            runningFunction.Dispose();
                        }
                    );
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
                    _flowType,
                    $"{watchdogName} for '{_flowType}' failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }

    private async Task<IReadOnlyList<InstanceIdAndEpoch>> ReAssertCrashedFunctions(IReadOnlyList<InstanceIdAndEpoch> eligibleFunctions, DateTime now)
    {
        //race-condition fix between re-invoker and lease-updater. Task.Delays are not respected when debugging.
        //fix is to allow lease updater to update lease before crashed watchdog asserts that the functions in question has crashed
        
        if (eligibleFunctions.Count == 0 || !Debugger.IsAttached)
            return eligibleFunctions;
        
        await Task.Delay(500);
        var eligibleFunctionsRepeated = 
            (await _getEligibleFunctions(_flowType, _functionStore, now.Ticks)).ToHashSet();
        
        return eligibleFunctions.Where(ie => eligibleFunctionsRepeated.Contains(ie)).ToList();
    }
}