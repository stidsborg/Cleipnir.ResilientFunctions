using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class ReInvoker
{
    public delegate Task<IReadOnlyList<InstanceIdAndEpoch>> GetEligibleFunctions(FunctionTypeId functionTypeId, IFunctionStore functionStore, long t);
    
    private readonly FunctionTypeId _functionTypeId;

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
        FunctionTypeId functionTypeId, 
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator, UnhandledExceptionHandler unhandledExceptionHandler, 
        TimeSpan checkFrequency, TimeSpan delayStartUp, 
        AsyncSemaphore asyncSemaphore, 
        RestartFunction restartFunction, ScheduleReInvokeFromWatchdog scheduleReInvoke,
        GetEligibleFunctions getEligibleFunctions
    )
    {
        _functionTypeId = functionTypeId;
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

                var eligibleFunctions = await _getEligibleFunctions(_functionTypeId, _functionStore, DateTime.UtcNow.Ticks);
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
                    
                    var functionId = new FunctionId(_functionTypeId, sef.InstanceId);
                    var restartedFunction = await _restartFunction(functionId, sef.Epoch);
                    if (restartedFunction == null)
                    {
                        runningFunction.Dispose();
                        takenLock.Dispose();
                        break;
                    }

                    await _scheduleReInvoke(
                        sef.InstanceId,
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
                    _functionTypeId,
                    $"{watchdogName} for '{_functionTypeId}' failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }
}