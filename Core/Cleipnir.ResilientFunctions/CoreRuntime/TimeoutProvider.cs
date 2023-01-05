using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public interface ITimeoutProvider
{
    Task RegisterTimeout(string timeoutId, DateTime expiresIn);
    Task RegisterTimeout(string timeoutId, TimeSpan expiresIn);
    Task CancelTimeout(string timeoutId);
}

public class TimeoutProvider : ITimeoutProvider
{
    private readonly ITimeoutStore _timeoutStore;
    
    private readonly EventSourceWriter _eventSourceWriter;
    private readonly TimeSpan _timeoutCheckFrequency;
    private readonly HashSet<string> _localTimeouts = new();
    private readonly object _sync = new();

    private readonly FunctionId _functionId;

    public TimeoutProvider(FunctionId functionId, ITimeoutStore timeoutStore, EventSourceWriter eventSourceWriter, TimeSpan timeoutCheckFrequency)
    {
        _timeoutStore = timeoutStore;
        _eventSourceWriter = eventSourceWriter;
        _timeoutCheckFrequency = timeoutCheckFrequency;
        _functionId = functionId;
    }

    public async Task RegisterTimeout(string timeoutId, DateTime expiresIn)
    {
        expiresIn = expiresIn.ToUniversalTime();
        _ = RegisterLocalTimeout(timeoutId, expiresIn);
        await _timeoutStore.UpsertTimeout(new StoredTimeout(_functionId, timeoutId, expiresIn.Ticks));
    }

    private async Task RegisterLocalTimeout(string timeoutId, DateTime expiresAt)
    {
        var expiresIn = expiresAt - DateTime.UtcNow; 
        if (expiresIn > _timeoutCheckFrequency) return;

        lock (_sync)
            _localTimeouts.Add(timeoutId);
        
        await Task.Delay(TimeSpanHelper.Max(expiresIn, TimeSpan.Zero));
        
        lock (_sync)
            if (!_localTimeouts.Contains(timeoutId)) return;

        await _eventSourceWriter.AppendEvent(
            new Timeout(timeoutId, expiresAt),
            idempotencyKey: $"Timeout¤{timeoutId}",
            reInvokeImmediatelyIfSuspended: true
        );

        await CancelTimeout(timeoutId);
    }
    
    public Task RegisterTimeout(string timeoutId, TimeSpan expiresIn)
        => RegisterTimeout(timeoutId, expiresIn: DateTime.UtcNow.Add(expiresIn));

    public async Task CancelTimeout(string timeoutId)
    {
        lock (_sync)
            _localTimeouts.Remove(timeoutId); 
        
        await _timeoutStore.RemoveTimeout(_functionId, timeoutId);   
    }
}