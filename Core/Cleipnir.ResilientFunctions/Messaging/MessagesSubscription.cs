using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Messaging;

public sealed class MessagesSubscription : IDisposable
{
    private readonly Func<Task<IReadOnlyList<StoredMessage>>> _pullNewMessages;
    private readonly Func<ValueTask> _dispose;
    private bool _disposed;

    private readonly object _sync = new();

    public MessagesSubscription(Func<Task<IReadOnlyList<StoredMessage>>> pullNewMessages, Func<ValueTask> dispose)
    {
        _pullNewMessages = pullNewMessages;
        _dispose = dispose;
    }

    public Task<IReadOnlyList<StoredMessage>> PullNewEvents() => _pullNewMessages();

    public void Dispose()
    {
        lock (_sync)
            if (_disposed) return;
            else _disposed = true;
        
        _dispose();  
    } 
}