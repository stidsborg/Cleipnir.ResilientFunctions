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
    private readonly TimeSpan _cleanUpFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly TimeSpan _retentionPeriod;
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    
    public RetentionWatchdog(
        FlowType flowType,
        StoredType storedType,
        IFunctionStore functionStore,
        TimeSpan cleanUpFrequency,
        TimeSpan delayStartUp,
        TimeSpan retentionPeriod,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _flowType = flowType;
        _storedType = storedType;
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _cleanUpFrequency = cleanUpFrequency;
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
                    await _functionStore.GetSucceededFunctions(_storedType, completedBefore.Ticks);

                foreach (var eligibleFunction in eligibleFunctions.WithRandomOffset())
                {
                    var alreadyDeleted = !await _functionStore.DeleteFunction(new StoredId(_storedType, eligibleFunction));
                    if (alreadyDeleted)
                        break;
                }
                
                var timeElapsed = DateTime.UtcNow - now;
                var delay = (_cleanUpFrequency - timeElapsed).RoundUpToZero();

                await Task.Delay(delay);
            }
        }
        catch (Exception thrownException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    $"{nameof(RetentionWatchdog)} for '{_flowType}' failed - retrying in 5 seconds",
                    innerException: thrownException,
                    _flowType
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }
}