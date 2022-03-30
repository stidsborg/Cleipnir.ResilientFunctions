using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Utils.Monitor;

namespace ConsoleApp.SharedResource;

public class MonitorExample
{
    private IMonitor Monitor { get; }
    private ISharedResourceApi1 ResourceApi1 { get; }
    private ISharedResourceApi2 ResourceApi2 { get; }

    public async Task<Return> UpdateSubscription(string rFuncId, string resourceId, string value)
    {
        await using var @lock = await Monitor.Acquire(lockId: nameof(UpdateSubscription), keyId: rFuncId);
        if (@lock == null)
            return Postpone.For(10_000, inProcessWait: true);

        await ResourceApi1.SetValue(resourceId, value);
        await ResourceApi2.SetValue(resourceId, value);
        return Succeed.WithoutValue;
    }
}