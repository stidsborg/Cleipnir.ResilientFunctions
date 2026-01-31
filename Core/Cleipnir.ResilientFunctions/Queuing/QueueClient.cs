using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Queuing;

public class QueueClient(QueueManager queueManager, ISerializer serializer, UtcNow utcNow)
{
    public Task FetchMessages() => queueManager.FetchMessagesOnce();

    public Task<T> Pull<T>(Workflow workflow, EffectId parentId, Func<T, bool>? filter = null, TimeSpan? maxWait = null)  where T : class
        => Pull(filter, workflow, parentId, timeout: null, maxWait)!;
    public Task<T?> Pull<T>(Workflow workflow, EffectId parentId, TimeSpan timeout, Func<T, bool>? filter = null, TimeSpan? maxWait = null) where T : class
        => Pull(filter, workflow, parentId, utcNow().Add(timeout), maxWait);
    public Task<T?> Pull<T>(Workflow workflow, EffectId parentId, DateTime timeout, Func<T, bool>? filter = null, TimeSpan? maxWait = null) where T : class
        => Pull(filter, workflow, parentId, timeout, maxWait);

    public Task<Envelope> PullEnvelope<T>(Workflow workflow, EffectId parentId, Func<T, bool>? filter = null, TimeSpan? maxWait = null) where T : class
        => PullEnvelope(e => e.Message is T t && (filter?.Invoke(t) ?? true), workflow, parentId, timeout: null, maxWait)!;
    public Task<Envelope?> PullEnvelope<T>(Workflow workflow, EffectId parentId, TimeSpan timeout, Func<T, bool>? filter = null, TimeSpan? maxWait = null) where T : class
        => PullEnvelope(e => e.Message is T t && (filter?.Invoke(t) ?? true), workflow, parentId, utcNow().Add(timeout), maxWait);
    public Task<Envelope?> PullEnvelope<T>(Workflow workflow, EffectId parentId, DateTime timeout, Func<T, bool>? filter = null, TimeSpan? maxWait = null) where T : class
        => PullEnvelope(e => e.Message is T t && (filter?.Invoke(t) ?? true), workflow, parentId, timeout, maxWait);

    private async Task<T?> Pull<T>(Func<T, bool>? filter, Workflow workflow, EffectId parentId, DateTime? timeout, TimeSpan? maxWait) where T : class
    {
        var envelope = await PullEnvelope(e => e.Message is T t && (filter?.Invoke(t) ?? true), workflow, parentId, timeout, maxWait);
        return (T?)envelope?.Message;
    }

    private async Task<Envelope?> PullEnvelope(Func<Envelope, bool>? filter, Workflow workflow, EffectId parentId, DateTime? timeout, TimeSpan? maxWait)
    {
        var effect = workflow.Effect;
        var messageId = parentId.CreateChild(0);
        var typeId = parentId.CreateChild(1);
        var timeoutId = parentId.CreateChild(2);
        var receiverId = parentId.CreateChild(3);
        var senderId = parentId.CreateChild(4);

        if (!effect.Contains(messageId))
        {
            var result = await queueManager.Subscribe(
                messageId,
                envelope => filter?.Invoke(envelope) ?? true,
                timeout,
                timeoutId,
                maxWait
            );

            if (result == null)
            {
                await effect.Upsert<object?>(messageId, null, alias: null, flush: false);
                return null;
            }

            var envelope = result.Message;
            var effectResults = result.EffectResults;
            await effect.Upserts(
                effectResults.Concat(
                [
                    new EffectResult(messageId, envelope.Message, Alias: null),
                    new EffectResult(typeId, serializer.SerializeType(envelope.Message.GetType()), Alias: null),
                    new EffectResult(receiverId, envelope.Receiver, Alias: null),
                    new EffectResult(senderId, envelope.Sender, Alias: null)
                ]),
                flush: false
            );

            return envelope;
        }
        
        if (!effect.TryGet<byte[]>(typeId, out var typeNameBytes))
            return null; // timeout case - no message was received

        var type = serializer.ResolveType(typeNameBytes!)
                   ?? throw new TypeLoadException($"Type '{Convert.ToBase64String(typeNameBytes!)}' could not be resolved");
        if (!effect.TryGet(messageId, type!, out var message))
            throw new InvalidOperationException("Effect did not have message as expected");

        effect.TryGet<string?>(receiverId, out var receiver);
        effect.TryGet<string?>(senderId, out var sender);

        return new Envelope(message!, receiver, sender);
    }
}