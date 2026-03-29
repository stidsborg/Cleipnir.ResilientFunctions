using System;
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

    public Task<T> Pull<T>(Workflow workflow, EffectId parentId, Func<T, bool>? filter = null)  where T : class
        => Pull(filter, workflow, parentId, timeout: null)!;
    public Task<T?> Pull<T>(Workflow workflow, EffectId parentId, TimeSpan timeout, Func<T, bool>? filter = null) where T : class
        => Pull(filter, workflow, parentId, utcNow().Add(timeout));
    public Task<T?> Pull<T>(Workflow workflow, EffectId parentId, DateTime timeout, Func<T, bool>? filter = null) where T : class
        => Pull(filter, workflow, parentId, timeout);

    public Task<Envelope> PullEnvelope<T>(Workflow workflow, EffectId parentId, Func<T, bool>? filter = null) where T : class
        => PullEnvelope(e => e.Message is T t && (filter?.Invoke(t) ?? true), workflow, parentId, timeout: null)!;
    public Task<Envelope?> PullEnvelope<T>(Workflow workflow, EffectId parentId, TimeSpan timeout, Func<T, bool>? filter = null) where T : class
        => PullEnvelope(e => e.Message is T t && (filter?.Invoke(t) ?? true), workflow, parentId, utcNow().Add(timeout));
    public Task<Envelope?> PullEnvelope<T>(Workflow workflow, EffectId parentId, DateTime timeout, Func<T, bool>? filter = null) where T : class
        => PullEnvelope(e => e.Message is T t && (filter?.Invoke(t) ?? true), workflow, parentId, timeout);

    private async Task<T?> Pull<T>(Func<T, bool>? filter, Workflow workflow, EffectId parentId, DateTime? timeout) where T : class
    {
        var envelope = await PullEnvelope(e => e.Message is T t && (filter?.Invoke(t) ?? true), workflow, parentId, timeout);
        return (T?)envelope?.Message;
    }

    private async Task<Envelope?> PullEnvelope(Func<Envelope, bool>? filter, Workflow workflow, EffectId parentId, DateTime? timeout)
    {
        var effect = workflow.Effect;
        var messageId = parentId.CreateChild(0);
        var messageTypeId = parentId.CreateChild(1);
        var timeoutId = parentId.CreateChild(2);
        var receiverId = parentId.CreateChild(3);
        var senderId = parentId.CreateChild(4);

        if (timeout != null)
        {
            var timeoutTicks = await effect.CreateOrGet(
                timeoutId,
                timeout.Value.ToUniversalTime().Ticks,
                flush: false,
                alias: null
            );

            timeout = new DateTime(timeoutTicks, DateTimeKind.Utc);
        }


        if (!effect.Contains(messageId))
        {
            var result = await queueManager.Subscribe(
                envelope => filter?.Invoke(envelope) ?? true,
                timeout,
                timeoutId,
                messageId,
                messageTypeId,
                receiverId,
                senderId
            );

            if (result == null)
                await effect.Upsert<object?>(messageId, null, alias: null, flush: false);

            return result;
        }

        if (!effect.TryGet<byte[]>(messageTypeId, out var typeNameBytes))
            return null; // timeout case - no message was received

        var type = serializer.ResolveType(typeNameBytes!)
                   ?? throw new TypeLoadException($"Type '{Convert.ToBase64String(typeNameBytes!)}' could not be resolved");
        if (!effect.TryGet<byte[]>(messageId, out var messageBytes))
            throw new InvalidOperationException("Effect did not contain message");

        var message = serializer.Deserialize(messageBytes!, type);

        effect.TryGet<string?>(receiverId, out var receiver);
        effect.TryGet<string?>(senderId, out var sender);

        return new Envelope(message, receiver, sender);
    }
}