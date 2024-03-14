using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public interface ITimeoutProvider
{
    Task RegisterTimeout(string timeoutId, DateTime expiresAt, bool overwrite = false);
    Task RegisterTimeout(string timeoutId, TimeSpan expiresIn, bool overwrite = false)
        => RegisterTimeout(timeoutId, DateTime.UtcNow.Add(expiresIn), overwrite);
    Task CancelTimeout(string timeoutId);
    IReadOnlyDictionary<TimeoutId, DateTime> PendingTimeouts();
}

public class TimeoutProvider : ITimeoutProvider
{
    private readonly ITimeoutStore _timeoutStore;
    
    private readonly MessageWriter? _messageWriter;
    private readonly TimeSpan _timeoutCheckFrequency;

    private ImmutableDictionary<TimeoutId, DateTime> _pendingTimeouts;
    private readonly object _sync = new();

    private readonly FunctionId _functionId;

    public TimeoutProvider(
        FunctionId functionId, 
        ITimeoutStore timeoutStore, 
        MessageWriter? messageWriter,
        IEnumerable<TimeoutEvent> pendingTimeouts,
        TimeSpan timeoutCheckFrequency)
    {
        _timeoutStore = timeoutStore;
        _messageWriter = messageWriter;
        
        _functionId = functionId;
        _timeoutCheckFrequency = timeoutCheckFrequency;
        _pendingTimeouts = pendingTimeouts.ToImmutableDictionary(
            t => new TimeoutId(t.TimeoutId),
            t => t.Expiration
        );
    }

    public async Task RegisterTimeout(string timeoutId, DateTime expiresAt, bool overwrite = false)
    {
        lock (_sync)
            if (_pendingTimeouts.ContainsKey(new TimeoutId(timeoutId)) && !overwrite)
                return;
            else
                _pendingTimeouts = _pendingTimeouts.SetItem(new TimeoutId(timeoutId), expiresAt);
        
        expiresAt = expiresAt.ToUniversalTime();
        await _timeoutStore.UpsertTimeout(new StoredTimeout(_functionId, timeoutId, expiresAt.Ticks), overwrite);
        _ = Task.Run(() => RegisterLocalTimeout(timeoutId, expiresAt));
    }

    private async Task RegisterLocalTimeout(string timeoutId, DateTime expiresAt)
    {
        if (_messageWriter == null) return;
        
        var expiresIn = expiresAt - DateTime.UtcNow; 
        if (expiresIn > _timeoutCheckFrequency) return;
        
        await Task.Delay(TimeSpanHelper.Max(expiresIn, TimeSpan.Zero));
        
        await _messageWriter.AppendMessage(
            new TimeoutEvent(timeoutId, expiresAt),
            idempotencyKey: $"Timeout¤{timeoutId}"
        );

        await CancelTimeout(timeoutId);
    }
    
    public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn, bool overwrite = false)
        => RegisterTimeout(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn), overwrite);

    public async Task CancelTimeout(string timeoutId)
    {
        await _timeoutStore.RemoveTimeout(_functionId, timeoutId);
        
        lock (_sync)
            _pendingTimeouts = _pendingTimeouts.Remove(new TimeoutId(timeoutId)); 
    }

    public IReadOnlyDictionary<TimeoutId, DateTime> PendingTimeouts()
    {
        lock (_sync)
            return _pendingTimeouts;
    }
}