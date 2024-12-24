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
            await effect.Remove(effectId);

        var enqueued = await store.SemaphoreStore.Release(existingSemaphore.Group, existingSemaphore.Instance, storedId, maximumCount);
        if (enqueued.Any())
            await store.Interrupt(enqueued);
    }

    private record ExistingSemaphoreAndEffectId(ExistingSemaphore ExistingSemaphore, string EffectId);
    private async Task<IReadOnlyList<ExistingSemaphoreAndEffectId>> GetExistingSemaphoreAndEffectIds()
    {
        var effectIds = await effect.AllIds;
        var semaphoreEffectIds = effectIds.Select(id => id.Id).Where(id => id.StartsWith("Semaphore#"));
        
        var existingSemaphoreAndEffectIds = new List<ExistingSemaphoreAndEffectId>(); 
        foreach (var semaphoreEffectId in semaphoreEffectIds)
        {
            var (group, instance, _) = (await effect.GetValue<SemaphoreIdAndStatus>(semaphoreEffectId))!;
            var existingSemaphore = new ExistingSemaphore(group, instance);
            var existingSemaphoreAndEffectId = new ExistingSemaphoreAndEffectId(existingSemaphore, semaphoreEffectId);
            existingSemaphoreAndEffectIds.Add(existingSemaphoreAndEffectId);
        }

        return existingSemaphoreAndEffectIds;
    }
}