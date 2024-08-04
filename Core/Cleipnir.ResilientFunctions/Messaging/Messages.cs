using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Reactive;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Messages : IReactiveChain<object> 
{
    public Timeouts Timeouts { get; }
    public IReactiveChain<object> Source => _messagePullerAndEmitter.Source;
    
    private readonly MessageWriter _messageWriter;
    private readonly MessagesPullerAndEmitter _messagePullerAndEmitter;
    
    public Messages(
        MessageWriter messageWriter,
        Timeouts timeouts,
        MessagesPullerAndEmitter messagePullerAndEmitter
    )
    {
        _messageWriter = messageWriter;
        Timeouts = timeouts;
        _messagePullerAndEmitter = messagePullerAndEmitter;
    }

    public async Task AppendMessage(object @event, string? idempotencyKey = null)
    {
        await _messageWriter.AppendMessage(@event, idempotencyKey);
        await Sync();
    }

    public Task Sync() => _messagePullerAndEmitter.PullEvents(maxSinceLastSynced: TimeSpan.Zero);

    public ISubscription Subscribe(Action<object> onNext, Action onCompletion, Action<Exception> onError) 
        => _messagePullerAndEmitter.Source.Subscribe(onNext, onCompletion, onError);
}