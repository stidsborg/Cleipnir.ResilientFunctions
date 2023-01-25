using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Utils.Monitor;

namespace ConsoleApp.SharedResource;

public class MonitorExample
{
    private IMonitor Monitor { get; }
    private ISharedResourceApi1 ResourceApi1 { get; }
    private ISharedResourceApi2 ResourceApi2 { get; }

    public MonitorExample(IMonitor monitor, ISharedResourceApi1 resourceApi1, ISharedResourceApi2 resourceApi2)
    {
        Monitor = monitor;
        ResourceApi1 = resourceApi1;
        ResourceApi2 = resourceApi2;
    }

    public async Task<Result> Invoke(string id, string resourceId, string value)
    {
        await using var @lock = await Monitor.Acquire(group: nameof(MonitorExample), key: id);
        if (@lock == null)
            return Postpone.For(10_000);

        await ResourceApi1.SetValue(resourceId, value);
        await ResourceApi2.SetValue(resourceId, value);
        return Succeed.WithoutValue;
    }
}