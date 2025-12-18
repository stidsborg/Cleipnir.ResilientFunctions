using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Queuing;

public class QueueClient(QueueManager queueManager, UtcNow utcNow)
{
    public Task<T?> Pull<T>(Workflow workflow, EffectId parentId, TimeSpan? timeout = null)
        => Pull<T>(workflow, parentId, timeout == null ? null : utcNow().Add(timeout.Value));

    public async Task<T?> Pull<T>(Workflow workflow, EffectId parentId, DateTime? timeout = null)
    {
        var effect = workflow.Effect;
        var valueId = parentId.CreateChild(0);
        var typeId = parentId.CreateChild(1);
        var timeoutId = parentId.CreateChild(2);
        
        if (!effect.Contains(valueId))
        {
            var result = await queueManager.Subscribe(
                valueId, 
                m => m is T, 
                timeout
            );

            if (result == null)
            {
                /*
                await effect.Upserts([
                    new EffectResult(valueId, null, Alias: null), //todo how to align this with bytes already from received message
                    new EffectResult(typeId, message.GetType().SimpleQualifiedName(), Alias: null)
                ], flush: false);
                */
                return default;                
            }

            var (message, effectResult) = result;
            await effect.Upserts([
                effectResult,
                new EffectResult(valueId, message, Alias: null), //todo how to align this with bytes already from received message
                new EffectResult(typeId, message.GetType().SimpleQualifiedName(), Alias: null)
            ], flush: false);

            return (T)message;
        }
        else
        {
            var type = Type.GetType(effect.Get<string>(typeId), throwOnError: true);
            var option = effect.TryGet(valueId, type!);

            if (!option.HasValue)
                throw new InvalidOperationException("Effect did not have value as expected");

            return (T)option.Value!;
        }
    }
}