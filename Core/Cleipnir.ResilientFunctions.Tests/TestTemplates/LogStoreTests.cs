using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class LogStoreTests
{
    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IFunctionStore> storeTask)
    {
        var logStore = (await storeTask).LogStore;
        var storedId = TestStoredId.Create();

        var entries = await logStore.GetEntries(storedId);
        entries.ShouldBeEmpty();

        var owner1 = new Owner(1);
        var msg1 = "hallo world".ToUtf8Bytes();
        var position1 = await logStore.Append(storedId, msg1, owner1);
        var msg2 = "hallo again".ToUtf8Bytes();
        var position2 = await logStore.Append(storedId, msg2, owner1);
        var owner2 = new Owner(2);
        var msg3 = "hallo from owner2".ToUtf8Bytes();
        var position3 = await logStore.Append(storedId, msg3, owner2);

        entries = await logStore.GetEntries(storedId);
        entries.Count.ShouldBe(3);
        
        entries[0].Position.ShouldBe(position1);
        entries[0].Owner.ShouldBe(owner1);
        entries[0].Content.ShouldBe(msg1);
        
        entries[1].Position.ShouldBe(position2);
        entries[1].Owner.ShouldBe(owner1);
        entries[1].Content.ShouldBe(msg2);
        
        entries[2].Position.ShouldBe(position3);
        entries[2].Owner.ShouldBe(owner2);
        entries[2].Content.ShouldBe(msg3);

        await logStore.Delete(storedId, position1);
        entries = await logStore.GetEntries(storedId);
        entries.Count.ShouldBe(2);
        
        entries[0].Position.ShouldBe(position2);
        entries[0].Owner.ShouldBe(owner1);
        entries[0].Content.ShouldBe(msg2);
        
        entries[1].Position.ShouldBe(position3);
        entries[1].Owner.ShouldBe(owner2);
        entries[1].Content.ShouldBe(msg3);
    }
    
    public abstract Task GetEntriesWithOffsetTest();
    protected async Task GetEntriesWithOffsetTest(Task<IFunctionStore> storeTask)
    {
        var logStore = (await storeTask).LogStore;
        var storedId = TestStoredId.Create();
        
        var owner1 = new Owner(1);
        var msg1 = "hallo world".ToUtf8Bytes();
        var position1 = await logStore.Append(storedId, msg1, owner1);
        var msg2 = "hallo again".ToUtf8Bytes();
        var position2 = await logStore.Append(storedId, msg2, owner1);
        var owner2 = new Owner(2);
        var msg3 = "hallo from owner2".ToUtf8Bytes();
        var position3 = await logStore.Append(storedId, msg3, owner2);

        var entries = await logStore.GetEntries(storedId, position1);
        entries.Count.ShouldBe(2);
        var (entryOwner, entryPosition, entryContent) = entries[0];
        entryOwner.ShouldBe(owner1);
        entryPosition.ShouldBe(position2);
        entryContent.ShouldBe(msg2);
        (entryOwner, entryPosition, entryContent) = entries[1];
        entryOwner.ShouldBe(owner2);
        entryPosition.ShouldBe(position3);
        entryContent.ShouldBe(msg3);

        entries = await logStore.GetEntries(storedId, position3);
        entries.ShouldBeEmpty();
    }
    
    public abstract Task GetEntriesWithOffsetAndOwnerTest();
    protected async Task GetEntriesWithOffsetAndOwnerTest(Task<IFunctionStore> storeTask)
    {
        var logStore = (await storeTask).LogStore;
        var storedId = TestStoredId.Create();
        
        var owner1 = new Owner(1);
        var msg1 = "hallo world".ToUtf8Bytes();
        var position1 = await logStore.Append(storedId, msg1, owner1);
        var msg2 = "hallo again".ToUtf8Bytes();
        var position2 = await logStore.Append(storedId, msg2, owner1);
        var owner2 = new Owner(2);
        var msg3 = "hallo from owner2".ToUtf8Bytes();
        var position3 = await logStore.Append(storedId, msg3, owner2);

        var (maxPosition, entries) = await logStore.GetEntries(storedId, position1, owner1);
        maxPosition.ShouldBe(position3);
        entries.Count.ShouldBe(1);
        var (entryOwner, entryPosition, entryContent) = entries.Single();
        entryOwner.ShouldBe(owner1);
        entryPosition.ShouldBe(position2);
        entryContent.ShouldBe(msg2);

        var secondGetEntries = await logStore.GetEntries(storedId, maxPosition, owner1);
        secondGetEntries.Entries.ShouldBeEmpty();
        secondGetEntries.MaxPosition.ShouldBe(maxPosition);
    }
}