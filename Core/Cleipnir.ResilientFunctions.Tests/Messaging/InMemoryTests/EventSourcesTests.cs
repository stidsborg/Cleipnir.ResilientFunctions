using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class EventSourcesTests : TestTemplates.EventSourcesTests
{
    [TestMethod]
    public override Task EventSourcesSunshineScenario() 
        => EventSourcesSunshineScenario(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task ExistingEventsShouldBeSameAsAllAfterEmit()
        => ExistingEventsShouldBeSameAsAllAfterEmit(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnored()
        => SecondEventWithExistingIdempotencyKeyIsIgnored(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully()
        => EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task EventSourcesSunshineScenarioUsingEventStore()
        => EventSourcesSunshineScenarioUsingEventStore(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore()
        => SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore(
            new InMemoryEventStore().CastTo<IEventStore>().ToTask()
        );

    [TestMethod]
    public override Task EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations()
        => EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(
            new InMemoryEventStore().CastTo<IEventStore>().ToTask()
        );
}