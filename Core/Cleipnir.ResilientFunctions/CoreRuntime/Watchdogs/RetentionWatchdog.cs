using System;
using System.Linq;
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
    
    private readonly UtcNow _utcNow;
    
    public RetentionWatchdog(
        FlowType flowType,
        StoredType storedType,
        IFunctionStore functionStore,
        TimeSpan cleanUpFrequency,
        TimeSpan delayStartUp,
        TimeSpan retentionPeriod,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator,
        UtcNow utcNow)
    {
        _flowType = flowType;
        _storedType = storedType;
        _functionStore = functionStore;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
        _cleanUpFrequency = cleanUpFrequency;
        _delayStartUp = delayStartUp;
        _retentionPeriod = retentionPeriod;
        _utcNow = utcNow;
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
                var now = _utcNow();
                var completedBefore = now - _retentionPeriod;
                var eligibleFunctions = await _functionStore.GetSucceededFunctions(completedBefore.Ticks);
                eligibleFunctions = eligibleFunctions.Where(id => id.Type == _storedType).ToList();
                foreach (var eligibleId in eligibleFunctions.WithRandomOffset())
                {
                    var alreadyDeleted = !await _functionStore.DeleteFunction(eligibleId);
                    if (alreadyDeleted)
                        break;
                }
                
                var timeElapsed = _utcNow() - now;
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