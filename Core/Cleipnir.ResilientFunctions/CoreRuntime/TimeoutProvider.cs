using System;
using System.Collections.Generic;
using System.Linq;
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
    Task RegisterTimeout(string timeoutId, TimeSpan expiresIn, bool overwrite = false);
    Task CancelTimeout(string timeoutId);
    Task<List<TimeoutEvent>> PendingTimeouts();
}

public class TimeoutProvider : ITimeoutProvider
{
    private readonly ITimeoutStore _timeoutStore;
    
    private readonly MessageWriter? _messageWriter;
    private readonly TimeSpan _timeoutCheckFrequency;
    private readonly HashSet<string> _localTimeouts = new();
    private readonly object _sync = new();

    private readonly FlowId _flowId;

    public TimeoutProvider(FlowId flowId, ITimeoutStore timeoutStore, MessageWriter? messageWriter, TimeSpan timeoutCheckFrequency)
    {
        _timeoutStore = timeoutStore;
        _messageWriter = messageWriter;
        _timeoutCheckFrequency = timeoutCheckFrequency;
        _flowId = flowId;
    }

    public async Task RegisterTimeout(string timeoutId, DateTime expiresAt, bool overwrite = false)
    {
        lock (_sync)
            if (_localTimeouts.Contains(timeoutId) && !overwrite)
                return;
        
        expiresAt = expiresAt.ToUniversalTime();
        _ = RegisterLocalTimeout(timeoutId, expiresAt);
        await _timeoutStore.UpsertTimeout(new StoredTimeout(_flowId, timeoutId, expiresAt.Ticks), overwrite);
    }
    
    public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn, bool overwrite = false)
        => RegisterTimeout(timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn), overwrite);

    private async Task RegisterLocalTimeout(string timeoutId, DateTime expiresAt)
    {
        if (_messageWriter == null) return;
        
        var expiresIn = expiresAt - DateTime.UtcNow; 
        if (expiresIn > _timeoutCheckFrequency) return;

        lock (_sync)
            _localTimeouts.Add(timeoutId);
        
        await Task.Delay(expiresIn.RoundUpToZero());
        
        lock (_sync)
            if (!_localTimeouts.Contains(timeoutId)) return;

        await _messageWriter.AppendMessage(
            new TimeoutEvent(timeoutId, expiresAt),
            idempotencyKey: $"Timeout¤{timeoutId}"
        );

        await CancelTimeout(timeoutId);
    }

    public async Task CancelTimeout(string timeoutId)
    {
        lock (_sync)
            _localTimeouts.Remove(timeoutId); 
        
        await _timeoutStore.RemoveTimeout(_flowId, timeoutId);   
    }

    public async Task<List<TimeoutEvent>> PendingTimeouts()
    {
        var timeouts = (await _timeoutStore.GetTimeouts(_flowId)).ToList();

        lock (_sync)
        {
            _localTimeouts.Clear();
            foreach (var timeout in timeouts) 
                _localTimeouts.Add(timeout.TimeoutId);
        }
        
        return timeouts
            .Where(t => t.FlowId == _flowId)
            .Select(t => new TimeoutEvent(t.TimeoutId, Expiration: new DateTime(t.Expiry).ToUniversalTime()))
            .ToList();
    }
}