using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Queuing;

public class QueueClient(QueueManager queueManager, UtcNow utcNow)
{
    public Task FetchMessages() => queueManager.FetchMessagesOnce();

    public Task<T> Pull<T>(Workflow workflow, EffectId parentId, Func<T, bool>? filter = null, TimeSpan? maxWait = null)  where T : class
        => Pull(filter, workflow, parentId, timeout: null, maxWait)!;
    public Task<T?> Pull<T>(Workflow workflow, EffectId parentId, TimeSpan timeout, Func<T, bool>? filter = null, TimeSpan? maxWait = null) where T : class
        => Pull(filter, workflow, parentId, utcNow().Add(timeout), maxWait);
    public Task<T?> Pull<T>(Workflow workflow, EffectId parentId, DateTime timeout, Func<T, bool>? filter = null, TimeSpan? maxWait = null) where T : class
        => Pull(filter, workflow, parentId, timeout, maxWait);

    private async Task<T?> Pull<T>(Func<T, bool>? filter, Workflow workflow, EffectId parentId, DateTime? timeout, TimeSpan? maxWait) where T : class
    {
        var effect = workflow.Effect;
        var valueId = parentId.CreateChild(0);
        var typeId = parentId.CreateChild(1);
        var timeoutId = parentId.CreateChild(2);
        
        if (!effect.Contains(valueId))
        {
            var result = await queueManager.Subscribe(
                valueId,
                m => m is T t && (filter?.Invoke(t) ?? true),
                timeout,
                timeoutId,
                maxWait
            );

            if (result == null)
            {
                await effect.Upsert<T?>(valueId, null, alias: null, flush: false);
                await effect.Upsert(typeId, typeof(T).SimpleQualifiedName(), alias: null, flush: false);
                return null;
            }

            var message = result.Message;
            var effectResults = result.EffectResults;
            await effect.Upserts(
                effectResults.Concat(
                [
                    new EffectResult(valueId, message, Alias: null), 
                    new EffectResult(typeId, message.GetType().SimpleQualifiedName(), Alias: null)
                ]),
                flush: false
            );

            return (T)message;
        }
        else
        {
            var type = Type.GetType(effect.Get<string>(typeId), throwOnError: true);
            if (!effect.TryGet(valueId, type!, out var value))
                throw new InvalidOperationException("Effect did not have value as expected");

            return (T)value!;
        }
    }
}