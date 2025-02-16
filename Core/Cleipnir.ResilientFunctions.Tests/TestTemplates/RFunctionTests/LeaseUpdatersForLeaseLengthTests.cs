using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class LeaseUpdatersForLeaseLengthTests
{
    public abstract Task LeaseUpdaterUpdatesExpiryForEligibleFlows();
    public async Task LeaseUpdaterUpdatesExpiryForEligibleFlows(Task<IFunctionStore> storeTask)
    {
        var beforeTicks = DateTime.UtcNow.Ticks;
        var leaseLength = TimeSpan.FromSeconds(120);
        var store = await storeTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var handler = new UnhandledExceptionHandler(unhandledExceptionHandler.Catch);
        var leaseUpdaters = new LeaseUpdatersForLeaseLength(leaseLength, store, handler);

        var id1 = TestStoredId.Create();
        await store.CreateFunction(id1, id1.ToString(), param: null, leaseExpiration: 0, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null).ShouldBeTrueAsync();
        
        var id2 = TestStoredId.Create();
        var id2Expires = 1_000_000_000_000_000_000L;
        await store.CreateFunction(id2, id2.ToString(), param: null, leaseExpiration: id2Expires, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null).ShouldBeTrueAsync();
        
        var id3 = TestStoredId.Create();
        await store.CreateFunction(id3, id3.ToString(), param: null, leaseExpiration: 0, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null).ShouldBeTrueAsync();
        
        leaseUpdaters.Set(id1, epoch: 0, expiresTicks: 0);
        leaseUpdaters.Set(id2, epoch: 0, expiresTicks: id2Expires);
        leaseUpdaters.Set(id3, epoch: 0, expiresTicks: 0);
        await leaseUpdaters.RenewLeases();

        var sf1 = await store.GetFunction(id1).ShouldNotBeNullAsync();
        sf1.Expires.ShouldBeGreaterThan(beforeTicks);
        
        var sf2 = await store.GetFunction(id2).ShouldNotBeNullAsync();
        sf2.Expires.ShouldBe(id2Expires);
        
        var sf3 = await store.GetFunction(id3).ShouldNotBeNullAsync();
        sf3.Expires.ShouldBeGreaterThan(beforeTicks);
        
        var executingFlows = leaseUpdaters.GetExecutingFlows();
        executingFlows.Count.ShouldBe(3);
        executingFlows[id1].Epoch.ShouldBe(0);
        executingFlows[id1].Expiry.ShouldBe(sf1.Expires);
        executingFlows[id2].Epoch.ShouldBe(0);
        executingFlows[id2].Expiry.ShouldBe(id2Expires);
        executingFlows[id3].Epoch.ShouldBe(0);
        executingFlows[id3].Expiry.ShouldBe(sf3.Expires);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task LeaseUpdatersRefreshedCorrectlyOnUnexpectedNumberOfAffectedFlows();
    public async Task LeaseUpdatersRefreshedCorrectlyOnUnexpectedNumberOfAffectedFlows(Task<IFunctionStore> storeTask)
    {
        var beforeTicks = DateTime.UtcNow.Ticks;
        var leaseLength = TimeSpan.FromSeconds(120);
        var store = await storeTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var handler = new UnhandledExceptionHandler(unhandledExceptionHandler.Catch);
        var leaseUpdaters = new LeaseUpdatersForLeaseLength(leaseLength, store, handler);

        var id1 = TestStoredId.Create();
        await store.CreateFunction(id1, id1.ToString(), param: null, leaseExpiration: 0, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null).ShouldBeTrueAsync();
        
        var id2 = TestStoredId.Create();
        var id2Expires = 1_000_000_000_000_000_000L;
        await store.CreateFunction(id2, id2.ToString(), param: null, leaseExpiration: id2Expires, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null).ShouldBeTrueAsync();
        
        var id3 = TestStoredId.Create();
        await store.CreateFunction(id3, id3.ToString(), param: null, leaseExpiration: 0, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null).ShouldBeTrueAsync();
        await store.RestartExecution(id3, expectedEpoch: 0, leaseExpiration: 0).ShouldNotBeNullAsync();
        
        leaseUpdaters.Set(id1, epoch: 0, expiresTicks: 0);
        leaseUpdaters.Set(id2, epoch: 0, expiresTicks: id2Expires);
        leaseUpdaters.Set(id3, epoch: 0, expiresTicks: 0);
        await leaseUpdaters.RenewLeases();

        var sf1 = await store.GetFunction(id1).ShouldNotBeNullAsync();
        sf1.Expires.ShouldBeGreaterThan(beforeTicks);
        
        var sf2 = await store.GetFunction(id2).ShouldNotBeNullAsync();
        sf2.Expires.ShouldBe(id2Expires);
        
        var sf3 = await store.GetFunction(id3).ShouldNotBeNullAsync();
        sf3.Expires.ShouldBe(0);

        var executingFlows = leaseUpdaters.GetExecutingFlows();
        executingFlows.Count.ShouldBe(2);
        executingFlows[id1].Epoch.ShouldBe(0);
        executingFlows[id1].Expiry.ShouldBe(sf1.Expires);
        executingFlows[id2].Epoch.ShouldBe(0);
        executingFlows[id2].Expiry.ShouldBe(id2Expires);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task LeaseUpdatersRepositoryThrowsResultsInUnhandledException();
    public async Task LeaseUpdatersRepositoryThrowsResultsInUnhandledException(Task<IFunctionStore> storeTask)
    {
        var leaseLength = TimeSpan.FromSeconds(120);
        var store = new CrashableFunctionStore(await storeTask);
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var handler = new UnhandledExceptionHandler(unhandledExceptionHandler.Catch);
        var leaseUpdaters = new LeaseUpdatersForLeaseLength(leaseLength, store, handler);

        var id1 = TestStoredId.Create();
        await store.CreateFunction(id1, id1.ToString(), param: null, leaseExpiration: 0, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null).ShouldBeTrueAsync();
        
        leaseUpdaters.Set(id1, epoch: 0, expiresTicks: 0);
        store.Crash();
        
        await leaseUpdaters.RenewLeases();
        
        unhandledExceptionHandler.ThrownExceptions.ShouldNotBeEmpty();
    }
    
    public abstract Task RunningLeaseUpdatersCanBeDisposed();
    public async Task RunningLeaseUpdatersCanBeDisposed(Task<IFunctionStore> storeTask)
    {
        var leaseLength = TimeSpan.FromSeconds(120);
        var store = new CrashableFunctionStore(await storeTask);
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var handler = new UnhandledExceptionHandler(unhandledExceptionHandler.Catch);
        var leaseUpdaters = new LeaseUpdatersForLeaseLength(leaseLength, store, handler);

        var leaseUpdatersTask = leaseUpdaters.Start();
        var stopWatch = Stopwatch.StartNew();
        await leaseUpdaters.DisposeAsync();
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(5));

        leaseUpdatersTask.Wait(TimeSpan.FromSeconds(1)); 
    }
    
    public abstract Task FilterOutContainsFiltersOutActiveFlows();
    public async Task FilterOutContainsFiltersOutActiveFlows(Task<IFunctionStore> storeTask)
    {
        var leaseLength = TimeSpan.FromSeconds(120);
        var store = await storeTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var handler = new UnhandledExceptionHandler(unhandledExceptionHandler.Catch);
        var leaseUpdaters = new LeaseUpdatersForLeaseLength(leaseLength, store, handler);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();
        
        leaseUpdaters.Set(id1, epoch: 0, expiresTicks: 0);
        leaseUpdaters.Set(id2, epoch: 0, expiresTicks: 0);

        var filtered = leaseUpdaters.FindAlreadyContains([
            new IdAndEpoch(id1, Epoch: 0),
            new IdAndEpoch(id3, Epoch: 0),
        ]);

        filtered.Single().FlowId.ShouldBe(id1);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task FilterOutContainsReturnsSameCollectionUnmodifiedWhenNoFilterIsPerformed();
    public async Task FilterOutContainsReturnsSameCollectionUnmodifiedWhenNoFilterIsPerformed(Task<IFunctionStore> storeTask)
    {
        var leaseLength = TimeSpan.FromSeconds(120);
        var store = await storeTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var handler = new UnhandledExceptionHandler(unhandledExceptionHandler.Catch);
        var leaseUpdaters = new LeaseUpdatersForLeaseLength(leaseLength, store, handler);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();
        
        leaseUpdaters.Set(id1, epoch: 0, expiresTicks: 0);

        var idAndEpoches = new List<IdAndEpoch>
        {
            new(id2, Epoch: 0),
            new(id3, Epoch: 0),
        };
        var filtered = leaseUpdaters.FindAlreadyContains(idAndEpoches);
        filtered.Count.ShouldBe(0);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task LeaseUpdatersFiltersOutAlreadyContains();
    public async Task LeaseUpdatersFiltersOutAlreadyContains(Task<IFunctionStore> storeTask)
    {
        var leaseLength120 = TimeSpan.FromSeconds(120);
        var leaseLength125 = TimeSpan.FromSeconds(125);
        var store = await storeTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var handler = new UnhandledExceptionHandler(unhandledExceptionHandler.Catch);
        var leaseUpdaters = new LeaseUpdaters(store, handler);

        var leaseUpdatersForLeaseLength120 = leaseUpdaters.GetOrCreateLeaseUpdatersForLeaseLength(leaseLength120);
        var leaseUpdatersForLeaseLength125 = leaseUpdaters.GetOrCreateLeaseUpdatersForLeaseLength(leaseLength125);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();
        var id4 = TestStoredId.Create();
        
        leaseUpdatersForLeaseLength120.Set(id1, epoch: 0, expiresTicks: 0);
        leaseUpdatersForLeaseLength125.Set(id2, epoch: 0, expiresTicks: 0);

        var idAndEpoches = new List<IdAndEpoch>
        {
            new(id1, Epoch: 0),
            new(id2, Epoch: 0),
            new(id3, Epoch: 0),
            new(id4, Epoch: 0),
        };
        var filtered = leaseUpdaters.FilterOutContains(idAndEpoches);
        filtered.Count.ShouldBe(2);
        filtered.Single(i => i.FlowId == id3).Epoch.ShouldBe(0);
        filtered.Single(i => i.FlowId == id4).Epoch.ShouldBe(0);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task LeaseUpdatersReturnsSameReferenceWhenFiltersWhenThereAreNoAlreadyContains();
    public async Task LeaseUpdatersReturnsSameReferenceWhenFiltersWhenThereAreNoAlreadyContains(Task<IFunctionStore> storeTask)
    {
        var leaseLength120 = TimeSpan.FromSeconds(120);
        var leaseLength125 = TimeSpan.FromSeconds(125);
        var store = await storeTask;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var handler = new UnhandledExceptionHandler(unhandledExceptionHandler.Catch);
        var leaseUpdaters = new LeaseUpdaters(store, handler);

        var leaseUpdatersForLeaseLength120 = leaseUpdaters.GetOrCreateLeaseUpdatersForLeaseLength(leaseLength120);
        var leaseUpdatersForLeaseLength125 = leaseUpdaters.GetOrCreateLeaseUpdatersForLeaseLength(leaseLength125);

        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();
        var id4 = TestStoredId.Create();
        
        leaseUpdatersForLeaseLength120.Set(id1, epoch: 0, expiresTicks: 0);
        leaseUpdatersForLeaseLength125.Set(id2, epoch: 0, expiresTicks: 0);

        var idAndEpoches = new List<IdAndEpoch>
        {
            new(id3, Epoch: 0),
            new(id4, Epoch: 0),
        };
        var filtered = leaseUpdaters.FilterOutContains(idAndEpoches);
        ReferenceEquals(idAndEpoches, filtered).ShouldBeTrue();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}