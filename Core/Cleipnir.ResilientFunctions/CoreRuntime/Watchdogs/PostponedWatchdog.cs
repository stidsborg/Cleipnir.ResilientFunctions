using System;
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
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly ScheduleReInvokeFromWatchdog _scheduleReInvoke;
    private readonly RestartFunction _restartFunction;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly FunctionTypeId _functionTypeId;

    private int _maxParallelismLeft;
    private readonly object _sync = new();

    public PostponedWatchdog(
        FunctionTypeId functionTypeId,
        IFunctionStore functionStore,
        ScheduleReInvokeFromWatchdog scheduleReInvoke,
        RestartFunction restartFunction,
        int maxParallelism,
        TimeSpan checkFrequency,
        TimeSpan delayStartUp,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _maxParallelismLeft = maxParallelism;
        _scheduleReInvoke = scheduleReInvoke;
        _restartFunction = restartFunction;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
    }

    public async Task Start()
    {
        if (_checkFrequency == TimeSpan.Zero) return;
        await Task.Delay(_delayStartUp);
        
        Start:
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var now = DateTime.UtcNow;

                var eligible = 
                    (await _functionStore.GetPostponedFunctions(_functionTypeId, now.Ticks)).WithRandomOffset();

                foreach (var spf in eligible)
                {
                    lock (_sync)
                        if (_maxParallelismLeft == 0) break;
                        else _maxParallelismLeft--;
                    
                    var runningFunction = _shutdownCoordinator.TryRegisterRunningFunction();
                    if (runningFunction == null)
                        return;

                    var functionId = new FunctionId(_functionTypeId, spf.InstanceId);
                    var restartedFunction = await _restartFunction(functionId, spf.Epoch);
                    if (restartedFunction == null)
                    {
                        runningFunction.Dispose();
                        break;
                    }

                    await _scheduleReInvoke(
                        spf.InstanceId,
                        restartedFunction,
                        onCompletion: () =>
                        {
                            lock (_sync)
                                _maxParallelismLeft++;
                            
                            runningFunction.Dispose();
                        }
                    );
                }
                
                var timeElapsed = DateTime.UtcNow - now;
                var delay = TimeSpanHelper.Max(
                    TimeSpan.Zero,
                    _checkFrequency - timeElapsed
                );

                await Task.Delay(delay);
            }
        }
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(PostponedWatchdog)} for '{_functionTypeId}' failed - retrying in 5 seconds",
                    innerException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }
}