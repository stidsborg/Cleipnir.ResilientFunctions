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
    public Task<T> Pull<T>(Workflow workflow, EffectId parentId, Func<T, bool>? filter = null) =>
        Pull<T>(filter, workflow, parentId, timeout: null)!;
    public Task<T?> Pull<T>(Workflow workflow, EffectId parentId, TimeSpan timeout, Func<T, bool>? filter = null)
        => Pull<T>(filter, workflow, parentId, utcNow().Add(timeout));
    public Task<T?> Pull<T>(Workflow workflow, EffectId parentId, DateTime timeout, Func<T, bool>? filter = null)
        => Pull<T>(filter, workflow, parentId, (DateTime?) timeout);
    
    private async Task<T?> Pull<T>(Func<T, bool>? filter, Workflow workflow, EffectId parentId, DateTime? timeout)
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
                timeoutId
            );

            if (result == null)
            {
                await effect.Upserts([
                    new EffectResult(valueId, default(T?), Alias: null), 
                    new EffectResult(typeId, typeof(T?).SimpleQualifiedName(), Alias: null)
                ], flush: false);
                return default;                
            }

            var (message, effectResult) = result;
            await effect.Upserts(effectResult.Concat([
                    new EffectResult(valueId, message, Alias: null), //todo how to align this with bytes already from received message
                    new EffectResult(typeId, message.GetType().SimpleQualifiedName(), Alias: null)
                ]),
                flush: false
            );

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