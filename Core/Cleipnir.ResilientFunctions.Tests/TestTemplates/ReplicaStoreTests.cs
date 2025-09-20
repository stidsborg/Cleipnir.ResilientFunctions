using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class ReplicaStoreTests
{
    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask.SelectAsync(s => s.ReplicaStore);
        await store.GetAll().ShouldBeEmptyAsync();
        var replicaId1 = Guid.NewGuid().ToReplicaId();
        var replicaId1Heartbeat = DateTime.UtcNow.Ticks;
        var replicaId2 = Guid.NewGuid().ToReplicaId();
        var replicaId2Heartbeat = DateTime.UtcNow.Ticks;
        
        {
            await store.Insert(replicaId1, replicaId1Heartbeat);
            var all = await store.GetAll();
            all.Count.ShouldBe(1);
            var stored = all.Single();
            stored.ReplicaId.ShouldBe(replicaId1);
            stored.LatestHeartbeat.ShouldBe(replicaId1Heartbeat);            
        }

        {
            await store.Insert(replicaId2, replicaId2Heartbeat);
            var all = await store.GetAll();
            all.Count.ShouldBe(2);
            var stored = all.Single(id => id.ReplicaId == replicaId2);
            stored.ReplicaId.ShouldBe(replicaId2);
            stored.LatestHeartbeat.ShouldBe(0);            
        }

        await store.UpdateHeartbeat(replicaId1, replicaId1Heartbeat + 1);
        {
            var all = await store.GetAll();
            all.Count.ShouldBe(2);
            var stored1 = all.Single(r => r.ReplicaId == replicaId1);
            stored1.LatestHeartbeat.ShouldBe(replicaId1Heartbeat + 1);
            var stored2 = all.Single(r => r.ReplicaId == replicaId2);
            stored2.LatestHeartbeat.ShouldBe(replicaId2Heartbeat);
        }
        
        await store.UpdateHeartbeat(replicaId2, replicaId2Heartbeat + 1);
        {
            var all = await store.GetAll();
            all.Count.ShouldBe(2);
            var stored1 = all.Single(r => r.ReplicaId == replicaId1);
            stored1.LatestHeartbeat.ShouldBe(replicaId1Heartbeat + 1);
            var stored2 = all.Single(r => r.ReplicaId == replicaId2);
            stored2.LatestHeartbeat.ShouldBe(replicaId2Heartbeat + 1);
        }

        await store.Delete(replicaId1);
        {
            var all = await store.GetAll();
            all.Count.ShouldBe(1);
            var stored2 = all.Single(r => r.ReplicaId == replicaId2);
            stored2.LatestHeartbeat.ShouldBe(replicaId2Heartbeat + 1);
        }
        
        await store.Delete(replicaId2);
        {
            var all = await store.GetAll();
            all.ShouldBeEmpty();
        }
    }
}