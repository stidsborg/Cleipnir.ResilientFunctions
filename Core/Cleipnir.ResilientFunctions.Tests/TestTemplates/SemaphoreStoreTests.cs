using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class SemaphoreStoreTests
{
    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<ISemaphoreStore> storeTask)
    {
        var store = await storeTask;
        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();
        var id4 = TestStoredId.Create();

        await store
            .Acquire("group", "instance", id1, maximumCount: 2)
            .ShouldBeTrueAsync();

        var takenIds = await store.GetQueued("group", "instance", count: 2);
        takenIds.Count.ShouldBe(1);
        takenIds[0].ShouldBe(id1);
        
        await store
            .Acquire("group", "instance", id2, maximumCount: 2)
            .ShouldBeTrueAsync();
        takenIds = await store.GetQueued("group", "instance", count: 2);
        takenIds.Count.ShouldBe(2);
        takenIds[0].ShouldBe(id1);
        takenIds[1].ShouldBe(id2);
        
        await store
            .Acquire("group", "instance", id3, maximumCount: 2)
            .ShouldBeFalseAsync();
        takenIds = await store.GetQueued("group", "instance", count: 2);
        takenIds.Count.ShouldBe(2);
        takenIds[0].ShouldBe(id1);
        takenIds[1].ShouldBe(id2);
        
        await store
            .Acquire("group", "instance", id4, maximumCount: 2)
            .ShouldBeFalseAsync();
        takenIds = await store.GetQueued("group", "instance", count: 2);
        takenIds.Count.ShouldBe(2);
        takenIds[0].ShouldBe(id1);
        takenIds[1].ShouldBe(id2);
        
        takenIds = await store.Release("group", "instance", id2, maximumCount: 2);
        takenIds.Count.ShouldBe(2);
        takenIds[0].ShouldBe(id1);
        takenIds[1].ShouldBe(id3);
        
        takenIds = await store.Release("group", "instance", id1, maximumCount: 2);
        takenIds.Count.ShouldBe(2);
        takenIds[0].ShouldBe(id3);
        takenIds[1].ShouldBe(id4);
        
        takenIds = await store.Release("group", "instance", id3, maximumCount: 2);
        takenIds.Count.ShouldBe(1);
        takenIds[0].ShouldBe(id4);
        
        takenIds = await store.Release("group", "instance", id4, maximumCount: 2);
        takenIds.Count.ShouldBe(0);
    }
    
    public abstract Task ReleasingSemaphoreTwiceSucceeds();
    protected async Task ReleasingSemaphoreTwiceSucceeds(Task<ISemaphoreStore> storeTask)
    {
        var store = await storeTask;
        var id1 = TestStoredId.Create();
        
        await store.Acquire("group", "instance", id1, maximumCount: 2);
        
        var takenIds = await store.Release("group", "instanc", id1, maximumCount: 2);
        takenIds.Count.ShouldBe(0);
        
        takenIds = await store.Release("group", "instanc", id1, maximumCount: 2);
        takenIds.Count.ShouldBe(0);
    }
    
    public abstract Task AcquiringTheSameSemaphoreTwiceIsIdempotent();
    protected async Task AcquiringTheSameSemaphoreTwiceIsIdempotent(Task<ISemaphoreStore> storeTask)
    {
        var store = await storeTask;
        var id1 = TestStoredId.Create();
        
        await store.Acquire("group", "instance", id1, maximumCount: 2).ShouldBeTrueAsync();
        await store.Acquire("group", "instance", id1, maximumCount: 2).ShouldBeTrueAsync();

        var takenIds = await store.GetQueued("group", "instance", count: 2);
        takenIds.Count.ShouldBe(1);
        takenIds[0].ShouldBe(id1);

        takenIds = await store.Release("group", "instanc", id1, maximumCount: 2);
        takenIds.Count.ShouldBe(0);
    }
    
    public abstract Task SemaphoreIsAcquiredDespitePositionBeingMoreThanSemaphoreCount();
    protected async Task SemaphoreIsAcquiredDespitePositionBeingMoreThanSemaphoreCount(Task<ISemaphoreStore> storeTask)
    {
        var store = await storeTask;
        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();
        
        
        await store.Acquire("group", "instance", id1, maximumCount: 2).ShouldBeTrueAsync();
        await store.Acquire("group", "instance", id2, maximumCount: 2).ShouldBeTrueAsync();

        await store.Release("group", "instance", id1, maximumCount: 2);
        await store.Acquire("group", "instance", id3, maximumCount: 2).ShouldBeTrueAsync();
        
        
    }
}