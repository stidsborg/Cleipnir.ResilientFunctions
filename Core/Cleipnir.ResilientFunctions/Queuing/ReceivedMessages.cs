using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

internal class ReceivedMessages(StoredId storedId, Effect effect, IMessageStore messageStore, IdempotencyKeys _idempotencyKeys, ISerializer serializer)
{
    private readonly EffectId _nextIndexId = new EffectId([-1, 1]);
    private readonly EffectId _messagesId = new EffectId([-1, 2]);
    private readonly EffectId _positionsToDeleteId = new EffectId([-1, -3]);
    private int _nextIndex = 0;
    private Dictionary<string, string> _x = new();

    private volatile bool _disposed;
    private readonly SemaphoreSlim _semaphoreSlim = new(1);

    private Dictionary<object, EffectId> _messages = new();
        
    public async Task Initialize()
    {
        effect.TryGet(_nextIndexId, out _nextIndex);
        _messages = effect
            .GetChildren(_messagesId)
            .Select(id => new { EffectId = id, Tuple = effect.Get<Tuple<byte[], byte[]>>(id) })
            .Select(a =>
                new KeyValuePair<object, EffectId>(
                    serializer.Deserialize(
                        a.Tuple.Item2, 
                        serializer.ResolveType(a.Tuple.Item1) ?? throw new TypeLoadException($"Unable to resolve type: '{Convert.ToBase64String(a.Tuple.Item1)}'")
                    ),
                    a.EffectId
                )
            ).ToDictionary(kv => kv.Key, kv => kv.Value);

        effect.TryGet<long[]>(_positionsToDeleteId, out var toDelete);
        if (toDelete != null && toDelete.Any())
        {
            await messageStore.DeleteMessages(storedId, toDelete);
            await effect.Upsert(_positionsToDeleteId, Array.Empty<long>(), alias: null, flush: false);
        }
    }

    public async Task FetchMessages()
    {
        if (_disposed)
            throw new ObjectDisposedException($"{nameof(QueueManager)} is disposed already");
        
        await _semaphoreSlim.WaitAsync();
        try
        {
            var messages = await messageStore.GetMessages(storedId, skip: 0); //todo create GetMessages without anything

            var positions = new List<long>(messages.Count);
            
            foreach (var (messageContent, messageType, position, idempotencyKey, sender, receiver) in messages)
            {
                try
                {
                    var msg = serializer.Deserialize(messageContent, serializer.ResolveType(messageType)!);

                    // NoOp messages are immediately deleted and not delivered
                    if (msg is NoOp)
                    {
                        await messageStore.DeleteMessages(storedId, [position]);
                        continue;
                    }

                    var idempotencyKeyResult = idempotencyKey == null
                        ? null
                        : _idempotencyKeys.Add(idempotencyKey, position);

                    if (idempotencyKey != null && idempotencyKeyResult == null)
                    {
                        await messageStore.DeleteMessages(storedId, [position]);
                        continue;
                    }

                    positions.Add(position);
                    
                    var envelope = new Envelope(msg, receiver, sender);
                    var envWithPosition = new QueueManager.EnvelopeWithPosition(envelope, position, idempotencyKeyResult);
                    lock (_lock)
                        _toDeliver.Add(envWithPosition);
                }
                catch (Exception e)
                {
                    unhandledExceptionHandler.Invoke(flowId.Type, e);
                    lock (_lock)
                        _skipPositions.Add(position);
                }
            }

            if (messages.Any())
                _ = TryToDeliverAsync();
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }
    
}