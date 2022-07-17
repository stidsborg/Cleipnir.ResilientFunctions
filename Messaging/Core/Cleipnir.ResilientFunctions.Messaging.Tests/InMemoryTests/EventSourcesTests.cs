using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Messaging.Tests.InMemoryTests;

[TestClass]
public class EventSourcesTests : Messaging.Tests.TestTemplates.EventSourcesTests
{
    [TestMethod]
    public override Task EventSourcesSunshineScenario() 
        => EventSourcesSunshineScenario(new InMemoryEventStore().CastTo<IEventStore>().ToTask());

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