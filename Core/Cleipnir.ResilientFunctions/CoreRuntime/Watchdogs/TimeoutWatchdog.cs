using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

internal class TimeoutWatchdog
{
    private readonly FunctionTypeId _functionTypeId;
    private readonly ITimeoutStore _timeoutStore;
    private readonly IEventStore _eventStore;
    private readonly ISerializer _serializer;
    private readonly TimeSpan _checkFrequency;
    private readonly TimeSpan _delayStartUp;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly ShutdownCoordinator _shutdownCoordinator;

    public TimeoutWatchdog(
        FunctionTypeId functionTypeId,
        ITimeoutStore timeoutStore, 
        IEventStore eventStore,
        ISerializer serializer,
        TimeSpan checkFrequency, 
        TimeSpan delayStartUp,
        UnhandledExceptionHandler unhandledExceptionHandler,
        ShutdownCoordinator shutdownCoordinator)
    {
        _functionTypeId = functionTypeId;
        _timeoutStore = timeoutStore;
        _eventStore = eventStore;
        _serializer = serializer;
        
        _checkFrequency = checkFrequency;
        _delayStartUp = delayStartUp;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _shutdownCoordinator = shutdownCoordinator;
    }

    public async Task Start()
    {
        if (_checkFrequency == TimeSpan.Zero) return;
        await Task.Delay(_delayStartUp);
        
        try
        {
            while (!_shutdownCoordinator.ShutdownInitiated)
            {
                var nextTimeoutSlot = DateTime.UtcNow.Add(_checkFrequency).Ticks;
                var upcomingTimeouts = await _timeoutStore.GetTimeouts(_functionTypeId.Value, nextTimeoutSlot);

                _ = HandleUpcomingTimeouts(upcomingTimeouts);
                
                await Task.Delay(_checkFrequency);
            }
        }
        catch (Exception innerException)
        {
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    _functionTypeId,
                    $"{nameof(TimeoutWatchdog)} failed while executing: '{_functionTypeId}'",
                    innerException
                )
            );
        }
    }

    private async Task HandleUpcomingTimeouts(IEnumerable<StoredTimeout> upcomingTimeouts)
    {
        foreach (var (functionId, timeoutId, expiry) in upcomingTimeouts.OrderBy(t => t.Expiry))
        {
            var expiryDateTime = new DateTime(expiry, DateTimeKind.Utc);
            var delay = TimeSpanHelper.Max(expiryDateTime - DateTime.UtcNow, TimeSpan.Zero);
            await Task.Delay(delay);
            var timeoutEvent = new Timeout(timeoutId, expiryDateTime);
            var (json, type) = _serializer.SerializeEvent(timeoutEvent);
            await _eventStore.AppendEvent(functionId, json, type, idempotencyKey: $"Timeout¤{timeoutId}");
        }
    }
}