using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public abstract class ReplicaWatchdogTests
{
    public abstract Task SunshineScenario();
    public async Task SunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask.SelectAsync(s => s.ReplicaStore);
        var replicaId1 = new ReplicaId(Guid.Parse("10000000-0000-0000-0000-000000000000"));
        using var watchdog1 = new ReplicaWatchdog(
            replicaId1,
            store,
            checkFrequency: TimeSpan.FromHours(1),
            onStrikeOut: _ => {}
        );
        await watchdog1.Initialize();
        var allReplicas = await store.GetAll();
        allReplicas.Count.ShouldBe(1);
        var storedReplica1 = allReplicas.Single(sr => sr.ReplicaId == replicaId1.Id);
        storedReplica1.Heartbeat.ShouldBe(0);
        
        var replicaId2 = new ReplicaId(Guid.Parse("20000000-0000-0000-0000-000000000000"));
        using var watchdog2 = new ReplicaWatchdog(
            replicaId2,
            store,
            checkFrequency: TimeSpan.FromHours(1),
            onStrikeOut: _ => {}
        );
        await watchdog2.Initialize();
        allReplicas = await store.GetAll();
        allReplicas.Count.ShouldBe(2);
        storedReplica1 = allReplicas.Single(sr => sr.ReplicaId == replicaId1.Id);
        storedReplica1.Heartbeat.ShouldBe(0);
        var storedReplica2 = allReplicas.Single(sr => sr.ReplicaId == replicaId2.Id);
        storedReplica2.Heartbeat.ShouldBe(0);

        await watchdog1.PerformIteration();
        var replicas = await store.GetAll();
        replicas.Single(sr => sr.ReplicaId == replicaId1.Id).Heartbeat.ShouldBe(1);
        replicas.Single(sr => sr.ReplicaId == replicaId2.Id).Heartbeat.ShouldBe(0);
        watchdog1.Strikes[new StoredReplica(replicaId2.Id, Heartbeat: 0)].ShouldBe(0);
        watchdog1.Strikes[new StoredReplica(replicaId1.Id, Heartbeat: 1)].ShouldBe(0);
        
        await watchdog1.PerformIteration();
        replicas = await store.GetAll();
        replicas.Single(sr => sr.ReplicaId == replicaId1.Id).Heartbeat.ShouldBe(2);
        replicas.Single(sr => sr.ReplicaId == replicaId2.Id).Heartbeat.ShouldBe(0);
        watchdog1.Strikes[new StoredReplica(replicaId2.Id, Heartbeat: 0)].ShouldBe(1);
        watchdog1.Strikes[new StoredReplica(replicaId1.Id, Heartbeat: 2)].ShouldBe(0);
        
        await watchdog1.PerformIteration();
        replicas = await store.GetAll();
        replicas.Count.ShouldBe(1);
        replicas.Single(sr => sr.ReplicaId == replicaId1.Id).Heartbeat.ShouldBe(3);
        watchdog1.Strikes.Count.ShouldBe(1);
        watchdog1.Strikes[new StoredReplica(replicaId1.Id, Heartbeat: 3)].ShouldBe(0);
    }
    
    public abstract Task ReplicaWatchdogStartResultsInAddedReplicaInStore();
    public async Task ReplicaWatchdogStartResultsInAddedReplicaInStore(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask.SelectAsync(s => s.ReplicaStore);
        var replicaId1 = new ReplicaId(Guid.Parse("10000000-0000-0000-0000-000000000000"));
        using var watchdog1 = new ReplicaWatchdog(
            replicaId1,
            store,
            checkFrequency: TimeSpan.FromHours(1),
            onStrikeOut: _ => {}
        );
        await watchdog1.Start();
        var allReplicas = await store.GetAll();
        allReplicas.Count.ShouldBe(1);
        
        var replicaId2 = new ReplicaId(Guid.Parse("20000000-0000-0000-0000-000000000000"));
        using var watchdog2 = new ReplicaWatchdog(
            replicaId2,
            store,
            checkFrequency: TimeSpan.FromHours(1),
            onStrikeOut: _ => {}
        );
        await watchdog2.Start();
        allReplicas = await store.GetAll();
        allReplicas.Count.ShouldBe(2);
    }
    
    public abstract Task StrikedOutReplicaIsRemovedFromStore();
    public async Task StrikedOutReplicaIsRemovedFromStore(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask.SelectAsync(s => s.ReplicaStore);
        var toBeStrikedOut = Guid.NewGuid();
        Guid? strikedOut = null;
        await store.Insert(toBeStrikedOut);
        var replicaId1 = new ReplicaId(Guid.Parse("10000000-0000-0000-0000-000000000000"));
        using var watchdog1 = new ReplicaWatchdog(
            replicaId1,
            store,
            checkFrequency: TimeSpan.FromHours(1),
            onStrikeOut: id => strikedOut = id
        );
        await watchdog1.Initialize();
        await watchdog1.PerformIteration();
        strikedOut.ShouldBeNull();
        await watchdog1.PerformIteration();
        strikedOut.ShouldBeNull();
        await watchdog1.PerformIteration();
        strikedOut.ShouldBe(toBeStrikedOut);

        var all = await store.GetAll();
        all.Count.ShouldBe(1);
        all.Single().ReplicaId.ShouldBe(replicaId1.Id);
    }
    
    public abstract Task RunningWatchdogUpdatesItsOwnHeartbeat();
    public async Task RunningWatchdogUpdatesItsOwnHeartbeat(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask.SelectAsync(s => s.ReplicaStore);
        var anyStrikesOut = false;
        var replicaId1 = new ReplicaId(Guid.NewGuid());
        using var watchdog1 = new ReplicaWatchdog(
            replicaId1,
            store,
            checkFrequency: TimeSpan.FromMilliseconds(100),
            onStrikeOut: _ => anyStrikesOut = true 
        );

        await watchdog1.Start();

        await BusyWait.Until(async () =>
        {
            var all = await store.GetAll();
            all.Count.ShouldBe(1);
            var single = all.Single();
            single.ReplicaId.ShouldBe(replicaId1.Id);
            return single.Heartbeat > 0;
        });
        
        anyStrikesOut.ShouldBe(false);
    }
    
    public abstract Task ReplicaIdOffsetIfCalculatedCorrectly();
    public async Task ReplicaIdOffsetIfCalculatedCorrectly(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask.SelectAsync(s => s.ReplicaStore);
        
        var replicaId1 = new ReplicaId(Guid.Parse("10000000-0000-0000-0000-000000000000"));
        var replicaId2 = new ReplicaId(Guid.Parse("20000000-0000-0000-0000-000000000000"));
        var replicaId3 = new ReplicaId(Guid.Parse("30000000-0000-0000-0000-000000000000"));

        var watchdog1 = new ReplicaWatchdog(replicaId1, store, checkFrequency: TimeSpan.FromHours(1), onStrikeOut: _ => { });
        var watchdog2 = new ReplicaWatchdog(replicaId2, store, checkFrequency: TimeSpan.FromHours(1), onStrikeOut: _ => { });
        var watchdog3 = new ReplicaWatchdog(replicaId3, store, checkFrequency: TimeSpan.FromHours(1), onStrikeOut: _ => { });

        await watchdog1.Initialize();
        await watchdog2.Initialize();
        await watchdog3.Initialize();
        
        await watchdog3.PerformIteration();
        replicaId3.Offset.ShouldBe(2);
        await watchdog2.PerformIteration();
        replicaId2.Offset.ShouldBe(1);
        await watchdog1.PerformIteration();
        replicaId1.Offset.ShouldBe(0);
    }
    
    public abstract Task NonExistingReplicaIdOffsetIsNull();
    public Task NonExistingReplicaIdOffsetIsNull(Task<IFunctionStore> storeTask)
    {
        var offset = ReplicaWatchdog.CalculateOffset(allReplicaIds: [], ownReplicaId: Guid.NewGuid());
        offset.ShouldBeNull();
        
        return Task.CompletedTask;
    }
}