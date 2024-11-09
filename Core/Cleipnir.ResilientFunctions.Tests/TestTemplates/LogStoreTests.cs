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
    }
}