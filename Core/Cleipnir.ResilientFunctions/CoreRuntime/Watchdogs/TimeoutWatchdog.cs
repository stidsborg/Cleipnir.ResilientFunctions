using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
    private readonly ITimeoutStore _timeoutStore;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    
    private volatile ImmutableDictionary<StoredType, MessageWriters> _messageWriters = ImmutableDictionary<StoredType, MessageWriters>.Empty;

    private bool _started = false;
    private readonly object _sync = new();

    public TimeoutWatchdog(
        ITimeoutStore timeoutStore,
        TimeSpan checkFrequency, 
        TimeSpan delayStartUp,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _timeoutStore = timeoutStore;

        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public void Register(StoredType storedType, MessageWriters messageWriters)
    {
        _messageWriters = _messageWriters.Add(storedType, messageWriters);
        
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
        var stopWatch = new Stopwatch();
        
        Start:
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var nextTimeoutSlot = DateTime.UtcNow.Add(_checkFrequency).Ticks;
                var upcomingTimeouts = await _timeoutStore.GetTimeouts(nextTimeoutSlot);

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
                    $"{nameof(TimeoutWatchdog)} failed while executing - retrying in 5 seconds",
                    innerException
                )
            );
            
            await Task.Delay(5_000);
            goto Start;
        }
    }

    private async Task HandleUpcomingTimeouts(IEnumerable<StoredTimeout> upcomingTimeouts)
    {
        var messageWriters = _messageWriters;
        foreach (var (functionId, timeoutId, expiry) in upcomingTimeouts.Where(t => messageWriters.ContainsKey(t.StoredId.StoredType)).OrderBy(t => t.Expiry))
        {
            var expiresAt = new DateTime(expiry, DateTimeKind.Utc);
            var delay = (expiresAt - DateTime.UtcNow).RoundUpToZero();
            await Task.Delay(delay);
            await messageWriters[functionId.StoredType].For(functionId.Instance).AppendMessage(new TimeoutEvent(timeoutId, expiresAt), idempotencyKey: $"Timeout¤{timeoutId}");
            await _timeoutStore.RemoveTimeout(functionId, timeoutId);
        }
    }
}