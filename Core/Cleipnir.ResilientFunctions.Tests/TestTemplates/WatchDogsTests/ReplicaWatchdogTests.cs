using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
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
        var functionStore = await storeTask;
        var store = functionStore.ReplicaStore;
        var replicaId1 = new ClusterInfo(Guid.Parse("10000000-0000-0000-0000-000000000000").ToReplicaId());
        using var watchdog1 = new ReplicaWatchdog(
            replicaId1,
            functionStore,
            checkFrequency: TimeSpan.FromHours(1),
            default(UnhandledExceptionHandler)!
        );
        await watchdog1.Initialize();
        var allReplicas = await store.GetAll();
        allReplicas.Count.ShouldBe(1);
        var storedReplica1 = allReplicas.Single(sr => sr.ReplicaId == replicaId1.ReplicaId);
        storedReplica1.Heartbeat.ShouldBe(0);
        
        var replicaId2 = new ClusterInfo(Guid.Parse("20000000-0000-0000-0000-000000000000").ToReplicaId());
        using var watchdog2 = new ReplicaWatchdog(
            replicaId2,
            functionStore,
            checkFrequency: TimeSpan.FromHours(1),
            default(UnhandledExceptionHandler)!
        );
        await watchdog2.Initialize();
        allReplicas = await store.GetAll();
        allReplicas.Count.ShouldBe(2);
        storedReplica1 = allReplicas.Single(sr => sr.ReplicaId == replicaId1.ReplicaId);
        storedReplica1.Heartbeat.ShouldBe(0);
        var storedReplica2 = allReplicas.Single(sr => sr.ReplicaId == replicaId2.ReplicaId);
        storedReplica2.Heartbeat.ShouldBe(0);

        await watchdog1.PerformIteration();
        var replicas = await store.GetAll();
        replicas.Single(sr => sr.ReplicaId == replicaId1.ReplicaId).Heartbeat.ShouldBe(1);
        replicas.Single(sr => sr.ReplicaId == replicaId2.ReplicaId).Heartbeat.ShouldBe(0);
        watchdog1.Strikes[new StoredReplica(replicaId2.ReplicaId, Heartbeat: 0)].ShouldBe(0);
        watchdog1.Strikes[new StoredReplica(replicaId1.ReplicaId, Heartbeat: 1)].ShouldBe(0);
        
        await watchdog1.PerformIteration();
        replicas = await store.GetAll();
        replicas.Single(sr => sr.ReplicaId == replicaId1.ReplicaId).Heartbeat.ShouldBe(2);
        replicas.Single(sr => sr.ReplicaId == replicaId2.ReplicaId).Heartbeat.ShouldBe(0);
        watchdog1.Strikes[new StoredReplica(replicaId2.ReplicaId, Heartbeat: 0)].ShouldBe(1);
        watchdog1.Strikes[new StoredReplica(replicaId1.ReplicaId, Heartbeat: 2)].ShouldBe(0);
        
        await watchdog1.PerformIteration();
        replicas = await store.GetAll();
        replicas.Count.ShouldBe(1);
        replicas.Single(sr => sr.ReplicaId == replicaId1.ReplicaId).Heartbeat.ShouldBe(3);
        watchdog1.Strikes.Count.ShouldBe(1);
        watchdog1.Strikes[new StoredReplica(replicaId1.ReplicaId, Heartbeat: 3)].ShouldBe(0);
    }
    
    public abstract Task ReplicaWatchdogStartResultsInAddedReplicaInStore();
    public async Task ReplicaWatchdogStartResultsInAddedReplicaInStore(Task<IFunctionStore> storeTask)
    {
        var functionStore = await storeTask;
        var store = functionStore.ReplicaStore;
        var replicaId1 = new ClusterInfo(Guid.Parse("10000000-0000-0000-0000-000000000000").ToReplicaId());
        using var watchdog1 = new ReplicaWatchdog(
            replicaId1,
            functionStore,
            checkFrequency: TimeSpan.FromHours(1),
            default(UnhandledExceptionHandler)!
        );
        await watchdog1.Start();
        var allReplicas = await store.GetAll();
        allReplicas.Count.ShouldBe(1);
        
        var replicaId2 = new ClusterInfo(Guid.Parse("20000000-0000-0000-0000-000000000000").ToReplicaId());
        using var watchdog2 = new ReplicaWatchdog(
            replicaId2,
            functionStore,
            checkFrequency: TimeSpan.FromHours(1),
            default(UnhandledExceptionHandler)!
        );
        await watchdog2.Start();
        allReplicas = await store.GetAll();
        allReplicas.Count.ShouldBe(2);
    }
    
    public abstract Task StrikedOutReplicaIsRemovedFromStore();
    public async Task StrikedOutReplicaIsRemovedFromStore(Task<IFunctionStore> storeTask)
    {
        var functionStore = await storeTask;
        var store = functionStore.ReplicaStore;
        var toBeStrikedOut = ReplicaId.NewId();
        
        await store.Insert(toBeStrikedOut);
        var replicaId1 = new ClusterInfo(Guid.Parse("10000000-0000-0000-0000-000000000000").ToReplicaId());
        using var watchdog1 = new ReplicaWatchdog(
            replicaId1,
            functionStore,
            checkFrequency: TimeSpan.FromHours(1),
            default(UnhandledExceptionHandler)!
        );
        await watchdog1.Initialize();
        await watchdog1.PerformIteration();
        await store.GetAll().SelectAsync(rs => rs.Count == 2).ShouldBeTrueAsync();
        await watchdog1.PerformIteration();
        await store.GetAll().SelectAsync(rs => rs.Count == 2).ShouldBeTrueAsync();
        await watchdog1.PerformIteration();

        var all = await store.GetAll();
        all.Count.ShouldBe(1);
        all.Single().ReplicaId.ShouldBe(replicaId1.ReplicaId);
    }
    
    public abstract Task RunningWatchdogUpdatesItsOwnHeartbeat();
    public async Task RunningWatchdogUpdatesItsOwnHeartbeat(Task<IFunctionStore> storeTask)
    {
        var functionStore = await storeTask;
        var store = functionStore.ReplicaStore;
        
        var replicaId1 = new ClusterInfo(ReplicaId.NewId());
        using var watchdog1 = new ReplicaWatchdog(
            replicaId1,
            functionStore,
            checkFrequency: TimeSpan.FromMilliseconds(100),
            default(UnhandledExceptionHandler)!
        );

        await watchdog1.Start();

        await BusyWait.Until(async () =>
        {
            var all = await store.GetAll();
            all.Count.ShouldBe(1);
            var single = all.Single();
            single.ReplicaId.ShouldBe(replicaId1.ReplicaId);
            return single.Heartbeat > 0;
        });
    }
    
    public abstract Task ReplicaIdOffsetIfCalculatedCorrectly();
    public async Task ReplicaIdOffsetIfCalculatedCorrectly(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var replicaStore = store.ReplicaStore;
        
        var replicaId1 = new ClusterInfo(Guid.Parse("10000000-0000-0000-0000-000000000000").ToReplicaId());
        var replicaId2 = new ClusterInfo(Guid.Parse("20000000-0000-0000-0000-000000000000").ToReplicaId());
        var replicaId3 = new ClusterInfo(Guid.Parse("30000000-0000-0000-0000-000000000000").ToReplicaId());

        var watchdog1 = new ReplicaWatchdog(replicaId1, store, checkFrequency: TimeSpan.FromHours(1), default(UnhandledExceptionHandler)!);
        var watchdog2 = new ReplicaWatchdog(replicaId2, store, checkFrequency: TimeSpan.FromHours(1), default(UnhandledExceptionHandler)!);
        var watchdog3 = new ReplicaWatchdog(replicaId3, store, checkFrequency: TimeSpan.FromHours(1), default(UnhandledExceptionHandler)!);

        await watchdog1.Initialize();
        await watchdog2.Initialize();
        await watchdog3.Initialize();
        
        await watchdog3.PerformIteration();
        replicaId3.Offset.ShouldBe((ulong) 2);
        await watchdog2.PerformIteration();
        replicaId2.Offset.ShouldBe((ulong) 1);
        await watchdog1.PerformIteration();
        replicaId1.Offset.ShouldBe((ulong) 0);
    }
    
    public abstract Task ReplicaIdOffsetIsUpdatedWhenNodeIsAddedAndDeleted();
    public async Task ReplicaIdOffsetIsUpdatedWhenNodeIsAddedAndDeleted(Task<IFunctionStore> storeTask)
    {
        var functionStore = await storeTask;
        var store = functionStore.ReplicaStore;
        
        var cluster1 = new ClusterInfo(Guid.Parse("10000000-0000-0000-0000-000000000000").ToReplicaId());
        var cluster2 = new ClusterInfo(Guid.Parse("20000000-0000-0000-0000-000000000000").ToReplicaId());
        var cluster3 = new ClusterInfo(Guid.Parse("30000000-0000-0000-0000-000000000000").ToReplicaId());

        var watchdog1 = new ReplicaWatchdog(cluster1, functionStore, checkFrequency: TimeSpan.FromHours(1), default(UnhandledExceptionHandler)!);
        var watchdog2 = new ReplicaWatchdog(cluster2, functionStore, checkFrequency: TimeSpan.FromHours(1), default(UnhandledExceptionHandler)!);
        var watchdog3 = new ReplicaWatchdog(cluster3, functionStore, checkFrequency: TimeSpan.FromHours(1), default(UnhandledExceptionHandler)!);

        await watchdog3.Initialize();
        cluster3.Offset.ShouldBe((ulong) 0);
        cluster3.ReplicaCount.ShouldBe((ulong) 1);

        await watchdog2.Initialize();
        await watchdog3.PerformIteration();
        cluster3.Offset.ShouldBe((ulong) 1);
        cluster3.ReplicaCount.ShouldBe((ulong) 2);
        cluster2.Offset.ShouldBe((ulong) 0);
        cluster2.ReplicaCount.ShouldBe((ulong) 2);
        
        await watchdog1.Initialize();
        await watchdog2.PerformIteration();
        await watchdog3.PerformIteration();
        cluster3.Offset.ShouldBe((ulong) 2);
        cluster3.ReplicaCount.ShouldBe((ulong) 3);
        cluster2.Offset.ShouldBe((ulong) 1);
        cluster2.ReplicaCount.ShouldBe((ulong) 3);
        cluster1.Offset.ShouldBe((ulong) 0);
        cluster1.ReplicaCount.ShouldBe((ulong) 3);

        await store.Delete(cluster1.ReplicaId);
        await watchdog3.PerformIteration();
        await watchdog2.PerformIteration();
        cluster3.Offset.ShouldBe((ulong) 1);
        cluster3.ReplicaCount.ShouldBe((ulong) 2);
        cluster2.Offset.ShouldBe((ulong) 0);
        cluster2.ReplicaCount.ShouldBe((ulong) 2);
        
        await store.Delete(cluster2.ReplicaId);
        await watchdog3.PerformIteration();
        cluster3.Offset.ShouldBe((ulong) 0);
        cluster3.ReplicaCount.ShouldBe((ulong) 1);
    }
    
    public abstract Task ActiveReplicasDoNotDeleteEachOther();
    public async Task ActiveReplicasDoNotDeleteEachOther(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var replicaStore = store.ReplicaStore;
        
        var cluster1 = new ClusterInfo(Guid.Parse("10000000-0000-0000-0000-000000000000").ToReplicaId());
        var cluster2 = new ClusterInfo(Guid.Parse("20000000-0000-0000-0000-000000000000").ToReplicaId());
        var cluster3 = new ClusterInfo(Guid.Parse("30000000-0000-0000-0000-000000000000").ToReplicaId());

        var watchdog1 = new ReplicaWatchdog(cluster1, store, checkFrequency: TimeSpan.FromHours(1), default(UnhandledExceptionHandler)!);
        var watchdog2 = new ReplicaWatchdog(cluster2, store, checkFrequency: TimeSpan.FromHours(1), default(UnhandledExceptionHandler)!);
        var watchdog3 = new ReplicaWatchdog(cluster3, store, checkFrequency: TimeSpan.FromHours(1), default(UnhandledExceptionHandler)!);

        await watchdog1.Initialize();
        await watchdog2.Initialize();
        await watchdog3.Initialize();

        await watchdog1.PerformIteration();
        await watchdog2.PerformIteration();
        
        await watchdog1.PerformIteration();
        await watchdog2.PerformIteration();
        
        await watchdog1.PerformIteration();
        await watchdog2.PerformIteration();
        
        await watchdog1.PerformIteration();
        await watchdog2.PerformIteration();

        var storedReplicas = await replicaStore.GetAll();
        storedReplicas.Count.ShouldBe(2);
        storedReplicas.Any(sr => sr.ReplicaId == cluster1.ReplicaId).ShouldBeTrue();
        storedReplicas.Any(sr => sr.ReplicaId == cluster2.ReplicaId).ShouldBeTrue();
    }
    
    public abstract Task NonExistingReplicaIdOffsetIsNull();
    public Task NonExistingReplicaIdOffsetIsNull(Task<IFunctionStore> storeTask)
    {
        var offset = ReplicaWatchdog.CalculateOffset(allReplicaIds: [], ownReplicaId: ReplicaId.NewId());
        offset.ShouldBeNull();
        
        return Task.CompletedTask;
    }
    
    public abstract Task StrikedOutReplicasFunctionIsPostponedAfterCrash();
    public async Task StrikedOutReplicasFunctionIsPostponedAfterCrash(Task<IFunctionStore> storeTask)
    {
        var functionStore = await storeTask;
        
        var toBeStrikedOut = ReplicaId.NewId();
        var storedId = TestStoredId.Create();
        await functionStore.CreateFunction(
            storedId,
            humanInstanceId: "SomeInstanceId",
            param: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: toBeStrikedOut
        ).ShouldBeTrueAsync();
        
        var store = functionStore.ReplicaStore;
        
        await store.Insert(toBeStrikedOut);
        var replicaId1 = new ClusterInfo(Guid.Parse("10000000-0000-0000-0000-000000000000").ToReplicaId());
        using var watchdog1 = new ReplicaWatchdog(
            replicaId1,
            functionStore,
            checkFrequency: TimeSpan.FromHours(1),
            default(UnhandledExceptionHandler)!
        );
        await watchdog1.Initialize();
        await watchdog1.PerformIteration();
        await watchdog1.PerformIteration();
        await watchdog1.PerformIteration();

        var sf = await functionStore.GetFunction(storedId).ShouldNotBeNullAsync();
        sf.Epoch.ShouldBe(1);
        sf.Status.ShouldBe(Status.Postponed);
        sf.Expires.ShouldBe(0);
        sf.OwnerId.ShouldBeNull();
    }
    
    public abstract Task ReplicaWatchdogUpdatesHeartbeat();
    public async Task ReplicaWatchdogUpdatesHeartbeat(Task<IFunctionStore> storeTask)
    {
        var functionStore = await storeTask;
        var replicaStore = functionStore.ReplicaStore;
        
        var replicaId = ReplicaId.NewId();
        var clusterInfo = new ClusterInfo(replicaId);
        using var watchdog = new ReplicaWatchdog(clusterInfo, functionStore, checkFrequency: TimeSpan.FromMilliseconds(10), default(UnhandledExceptionHandler)!);
        await watchdog.Start();

        await Task.Delay(100);
        
        var storedReplicas = await replicaStore.GetAll();
        storedReplicas.Count.ShouldBe(1);
        storedReplicas.Single().ReplicaId.ShouldBe(replicaId);
        storedReplicas.Single().Heartbeat.ShouldBeGreaterThan(1);

    }
    
    public abstract Task WorkIsDividedBetweenReplicas();
    public Task WorkIsDividedBetweenReplicas(Task<IFunctionStore> storeTask)
    {
        var replicaId1 = ReplicaId.NewId();
        var clusterInfo = new ClusterInfo(replicaId1)
        {
            ReplicaCount = 3
        };

        var storedIds = Enumerable
            .Range(0, 10)
            .Select(i => new StoredId(new StoredType(0), i.ToString().ToStoredInstance()))
            .ToList();

        //offset 0
        var owned = storedIds.Select(clusterInfo.OwnedByThisReplica).ToList();
        owned.Any().ShouldBeTrue();
        
        //offset 1
        clusterInfo.Offset = 1;
        owned = storedIds.Select(clusterInfo.OwnedByThisReplica).ToList();
        owned.Any().ShouldBeTrue();
        
        //offset 2
        clusterInfo.Offset = 2;
        owned = storedIds.Select(clusterInfo.OwnedByThisReplica).ToList();
        owned.Any().ShouldBeTrue();
        
        return Task.CompletedTask;
    }
}