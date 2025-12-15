using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Queuing;

public class QueueClient(QueueManager queueManager)
{
    public async Task<object> Pull<T>(Workflow workflow, EffectId parentId)
    {
        var effect = workflow.Effect;
        var valueId = parentId.CreateChild(0);
        var typeId = parentId.CreateChild(1);

        if (!effect.Contains(valueId))
        {
            var (message, result) = await queueManager.Subscribe(valueId, m => m is T);
            await effect.Upserts([
                result,
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