using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class ReplicaStoreTests
{
    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IReplicaStore> storeTask)
    {
        var store = await storeTask;

        await store.GetReplicaCount().ShouldBeAsync(0);

        var replica1Id = Guid.NewGuid();
        await store.Insert(replica1Id, ttl: 100);

        await store.GetReplicaCount().ShouldBeAsync(1);

        var replica2Id = Guid.NewGuid();
        await store.Insert(replica2Id, ttl: 200);
        
        await store.GetReplicaCount().ShouldBeAsync(2);

        await store.Delete(replica1Id);
        
        await store.GetReplicaCount().ShouldBeAsync(1);
        
        var replica3Id = Guid.NewGuid();
        await store.Insert(replica3Id, ttl: 300);

        await store.Update(replica2Id, ttl: 400);

        await store.Prune(currentTime: 350);

        await store.GetReplicaCount().ShouldBeAsync(1);
    }
}