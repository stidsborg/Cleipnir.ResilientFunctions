using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Reactive;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Messages : IReactiveChain<object> 
{
    public RegisteredTimeouts RegisteredTimeouts { get; }
    public IReactiveChain<object> Source => _messagePullerAndEmitter.Source;
    internal UtcNow UtcNow { get; }
    
    private readonly MessageWriter _messageWriter;
    private readonly MessagesPullerAndEmitter _messagePullerAndEmitter;
    
    public Messages(
        MessageWriter messageWriter,
        RegisteredTimeouts registeredTimeouts,
        MessagesPullerAndEmitter messagePullerAndEmitter,
        UtcNow utcNow
    )
    {
        _messageWriter = messageWriter;
        RegisteredTimeouts = registeredTimeouts;
        _messagePullerAndEmitter = messagePullerAndEmitter;
        UtcNow = utcNow;
    }

    public async Task AppendMessage(object @event, string? idempotencyKey = null)
    {
        await _messageWriter.AppendMessage(@event, idempotencyKey);
        await Sync();
    }

    public Task Sync() => _messagePullerAndEmitter.PullEvents(maxSinceLastSynced: TimeSpan.Zero);

    public ISubscription Subscribe(Action<object> onNext, Action onCompletion, Action<Exception> onError) 
        => _messagePullerAndEmitter.Source.Subscribe(onNext, onCompletion, onError);
    
    public IAsyncEnumerator<object> GetAsyncEnumerator(CancellationToken cancellationToken = default) => Source.GetAsyncEnumerator(cancellationToken); 
}