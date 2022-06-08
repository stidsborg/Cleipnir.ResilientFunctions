using System.Reactive.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL.Tests;

[TestClass]
public class EventSourcesTests
{
    [TestMethod]
    public async Task EventSourcesSunshineScenario()
    {
        var functionId = new FunctionId("TypeId", "InstanceId");
        var eventStore = await Sql.CreateAndInitializeEventStore(
            nameof(EventSourcesTests),
            nameof(EventSourcesSunshineScenario)
        );
        await eventStore.Initialize();
        var eventSources = new EventSources(eventStore);

        var eventSource = await eventSources.GetEventSource(functionId);

        async Task<object> FirstAsync() => await eventSource.All.FirstAsync();
        var task = FirstAsync();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await eventSource.Emit("hello world");

        (await task).ShouldBe("hello world");
    }
}