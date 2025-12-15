using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Queuing;

internal class IdempotencyKeys(EffectId id, Effect effect)
{
    public async Task Initialize()
    {
        effect.GetChildren(id);
        await Task.CompletedTask;
    }
}