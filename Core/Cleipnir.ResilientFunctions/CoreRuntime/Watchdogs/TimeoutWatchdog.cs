using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class TimeoutWatchdog
{
    private readonly FlowType _flowType;
    private readonly ITimeoutStore _timeoutStore;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly MessageWriters _messageWriters;

    public TimeoutWatchdog(
        FlowType flowType,
        MessageWriters messageWriters,
        ITimeoutStore timeoutStore,
        TimeSpan checkFrequency, 
        TimeSpan delayStartUp,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _flowType = flowType;
        _messageWriters = messageWriters;
        _timeoutStore = timeoutStore;

        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task Start()
    {
        await Task.Delay(_delayStartUp);
        var stopWatch = new Stopwatch();
        
        Start:
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var nextTimeoutSlot = DateTime.UtcNow.Add(_checkFrequency).Ticks;
                var upcomingTimeouts = await _timeoutStore.GetTimeouts(_flowType.Value, nextTimeoutSlot);

                stopWatch.Restart();
                await HandleUpcomingTimeouts(upcomingTimeouts);

                var delay = (_checkFrequency - stopWatch.Elapsed).RoundUpToZero(); ;
                await Task.Delay(delay);
            }
        }
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _flowType,
                    $"{nameof(TimeoutWatchdog)} failed while executing: '{_flowType}' - retrying in 5 seconds",
                    innerException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }

    private async Task HandleUpcomingTimeouts(IEnumerable<StoredTimeout> upcomingTimeouts)
    {
        foreach (var (functionId, timeoutId, expiry) in upcomingTimeouts.OrderBy(t => t.Expiry))
        {
            var expiresAt = new DateTime(expiry, DateTimeKind.Utc);
            var delay = (expiresAt - DateTime.UtcNow).RoundUpToZero();
            await Task.Delay(delay);
            await _messageWriters.For(functionId.Instance).AppendMessage(new TimeoutEvent(timeoutId, expiresAt), idempotencyKey: $"Timeout¤{timeoutId}");
            await _timeoutStore.RemoveTimeout(functionId, timeoutId);
        }
    }
}