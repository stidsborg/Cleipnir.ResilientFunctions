using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Origin;

namespace Cleipnir.ResilientFunctions.Messaging;

public class Messages : IReactiveChain<object>, IDisposable
{
    public TimeoutProvider TimeoutProvider { get; }
    public IReactiveChain<object> Source => _messagePullerAndEmitter.Source;
    public IEnumerable<object> Existing => _messagePullerAndEmitter.Source.Existing;
    
    private readonly MessageWriter _messageWriter;
    private readonly MessagesPullerAndEmitter _messagePullerAndEmitter;
    
    public Messages(
        FunctionId functionId,
        IMessageStore messageStore, 
        MessageWriter messageWriter,
        TimeoutProvider timeoutProvider,
        TimeSpan? pullFrequency, 
        ISerializer serializer)
    {
        _messageWriter = messageWriter;
        TimeoutProvider = timeoutProvider;
        
        _messagePullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            pullFrequency ?? TimeSpan.FromMilliseconds(250),
            messageStore,
            serializer,
            timeoutProvider
        );
    }

    public async Task AppendMessage(object @event, string? idempotencyKey = null)
    {
        await _messageWriter.AppendMessage(@event, idempotencyKey);
        await Sync();
    }

    public Task Sync() => _messagePullerAndEmitter.PullEvents();

    public void Dispose() => _messagePullerAndEmitter.Dispose();

    public ISubscription Subscribe(Action<object> onNext, Action onCompletion, Action<Exception> onError, int? subscriptionGroupId = null) 
        => _messagePullerAndEmitter.Source.Subscribe(onNext, onCompletion, onError, subscriptionGroupId);

    private class MessagesPullerAndEmitter : IDisposable
    {
        private readonly TimeSpan _delay;
        private readonly MessagesSubscription _messagesSubscription;
        private readonly ISerializer _serializer;

        public Source Source { get; }
        
        private Exception? _thrownException;
        private int _subscribers;
        private bool _running;
        private volatile bool _disposed;
        
        private readonly HashSet<string> _idempotencyKeys = new();
        
        private readonly AsyncSemaphore _semaphore = new(maxParallelism: 1);
        private readonly object _sync = new();
        
        public MessagesPullerAndEmitter(
            FunctionId functionId, 
            TimeSpan delay, 
            IMessageStore messageStore, ISerializer serializer, ITimeoutProvider timeoutProvider)
        {
            _delay = delay;

            _serializer = serializer;

            Source = new Source(
                timeoutProvider, 
                onSubscriptionCreated: SubscriberAdded,
                onSubscriptionRemoved: SubscriberRemoved
            );
            
            _messagesSubscription = messageStore.SubscribeToMessages(functionId);
        }
        
        private async Task StartPullEventLoop()
        {
            lock (_sync)
                if (_running)
                    return;
                else 
                    _running = true;
            
            try
            {
                while (true)
                {
                    lock (_sync)
                        if (_subscribers == 0)
                        {
                            _running = false;
                            return;
                        }
                    
                    await PullEvents();
                    
                    await Task.Delay(_delay);
                }                
            }
            catch (Exception)
            {
                // ignored
            }
        }

        private void SubscriberAdded()
        {
            lock (_sync)
                _subscribers++;
            
            Task.Run(StartPullEventLoop);
        }

        private void SubscriberRemoved()
        {
            lock (_sync)
                _subscribers--;
        }

        public async Task PullEvents()
        {
            lock (_sync)
                if (_disposed)
                    throw new ObjectDisposedException(nameof(Messages));
            
            using var @lock = await _semaphore.Take();
            if (_thrownException != null)
                throw new MessageProcessingException(_thrownException);
            
            try
            {
                var storedMessages = await _messagesSubscription.PullNewEvents();

                var filterStoredMessages = new List<StoredMessage>(storedMessages.Count);
                foreach (var storedMessage in storedMessages)
                    if (storedMessage.IdempotencyKey == null || !_idempotencyKeys.Contains(storedMessage.IdempotencyKey))
                    {
                        filterStoredMessages.Add(storedMessage);
                        if (storedMessage.IdempotencyKey != null)
                            _idempotencyKeys.Add(storedMessage.IdempotencyKey);
                    }
                
                storedMessages = filterStoredMessages;
                if (storedMessages.Count == 0)
                    return;
                
                var events = storedMessages.Select(
                    storedEvent => _serializer.DeserializeMessage(storedEvent.MessageJson, storedEvent.MessageType)
                );
                
                Source.SignalNext(events);
            }
            catch (Exception e)
            {
                var eventHandlingException = new MessageProcessingException(e);
                _thrownException = e;
                
                Source.SignalError(eventHandlingException);
                
                throw eventHandlingException;
            }
        }

        public void Dispose()
        {
            _disposed = true;
            _messagesSubscription.Dispose();
        }  
    }
}