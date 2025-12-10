using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public record ExistingSemaphore(string Group, string Instance);

public class ExistingSemaphores(StoredId storedId, IFunctionStore store, ExistingEffects effect)
{
    public async Task<IReadOnlyList<ExistingSemaphore>> GetAll() 
        => (await GetExistingSemaphoreAndEffectIds()).Select(e => e.ExistingSemaphore).ToList();
    
    public async Task ForceRelease(ExistingSemaphore existingSemaphore, int maximumCount)
    {
        var effectId = (await GetExistingSemaphoreAndEffectIds())
            .FirstOrDefault(e => e.ExistingSemaphore == existingSemaphore)
            ?.EffectId;

        if (effectId != null)
            await effect.Remove(effectId.Value);

        var enqueued = await store.SemaphoreStore.Release(existingSemaphore.Group, existingSemaphore.Instance, storedId, maximumCount);
        if (enqueued.Any())
            await store.Interrupt(enqueued);
    }

    private record ExistingSemaphoreAndEffectId(ExistingSemaphore ExistingSemaphore, int EffectId);
    private async Task<IReadOnlyList<ExistingSemaphoreAndEffectId>> GetExistingSemaphoreAndEffectIds()
    {
        var effectIds = await effect.AllIds;

        var existingSemaphoreAndEffectIds = new List<ExistingSemaphoreAndEffectId>();
        foreach (var effectId in effectIds)
        {
            try
            {
                var semaphoreData = await effect.GetValue<SemaphoreIdAndStatus>(effectId);
                if (semaphoreData != null)
                {
                    var (group, instance, _) = semaphoreData;
                    var existingSemaphore = new ExistingSemaphore(group, instance);
                    var existingSemaphoreAndEffectId = new ExistingSemaphoreAndEffectId(existingSemaphore, effectId.Id);
                    existingSemaphoreAndEffectIds.Add(existingSemaphoreAndEffectId);
                }
            }
            catch
            {
                // Effect is not a semaphore, skip it
            }
        }

        return existingSemaphoreAndEffectIds;
    }
}