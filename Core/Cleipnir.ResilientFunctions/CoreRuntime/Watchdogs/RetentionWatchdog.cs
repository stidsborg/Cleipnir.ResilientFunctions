using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class RetentionWatchdog
{
    private readonly IFunctionStore _functionStore;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly TimeSpan _retentionPeriod;
    private readonly FunctionTypeId _functionTypeId;
    
    public RetentionWatchdog(
        FunctionTypeId functionTypeId,
        IFunctionStore functionStore,
        TimeSpan checkFrequency,
        TimeSpan delayStartUp,
        TimeSpan retentionPeriod,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _retentionPeriod = retentionPeriod;
    }
    
    public async Task Start()
    {
        if (_retentionPeriod == TimeSpan.MaxValue) return;
        await Task.Delay(_delayStartUp);

        Start:
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var now = DateTime.UtcNow;
                var completedBefore = now - _retentionPeriod;
                var eligibleFunctions = 
                    await _functionStore.GetSucceededFunctions(_functionTypeId, completedBefore.Ticks);

                foreach (var eligibleFunction in eligibleFunctions.WithRandomOffset())
                {
                    var alreadyDeleted = !await _functionStore.DeleteFunction(new FunctionId(_functionTypeId, eligibleFunction));
                    if (alreadyDeleted)
                        break;
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
                    $"{nameof(RetentionWatchdog)} for '{_functionTypeId}' failed - retrying in 5 seconds",
                    innerException: thrownException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }
}