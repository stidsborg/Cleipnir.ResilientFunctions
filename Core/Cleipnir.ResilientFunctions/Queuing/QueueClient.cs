using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Queuing;

public class QueueClient(QueueManager queueManager)
{
    public async Task<object> Pull<T>()
    {
        var workflow = CurrentFlow.Workflow ?? throw new InvalidOperationException("Method must be called inside of an executing flow");
        var effect = workflow.Effect;

        var valueId = effect.CreateNextImplicitId();
        var typeId = effect.CreateNextImplicitId();

        if (!await effect.Contains(valueId))
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
            var type = Type.GetType(await effect.Get<string>(valueId), throwOnError: true);
            var option = await effect.TryGet(valueId, type!);

            if (!option.HasValue)
                throw new InvalidOperationException("Effect did not have value as expected");

            return (T)option.Value!;
        }
    }
}