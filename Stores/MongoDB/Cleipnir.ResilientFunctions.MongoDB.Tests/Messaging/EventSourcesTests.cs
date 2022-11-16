using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.Messaging;

[TestClass]
public class EventSourcesTests : ResilientFunctions.Tests.Messaging.TestTemplates.EventSourcesTests
{
    [TestMethod]
    public override Task EventSourcesSunshineScenario() 
        => EventSourcesSunshineScenario(NoSql.AutoCreateAndInitializeEventStore());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnored()
        => SecondEventWithExistingIdempotencyKeyIsIgnored(NoSql.AutoCreateAndInitializeEventStore());

    [TestMethod]
    public override Task EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully()
        => EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully(NoSql.AutoCreateAndInitializeEventStore());

    [TestMethod]
    public override Task EventSourcesSunshineScenarioUsingEventStore()
        => EventSourcesSunshineScenarioUsingEventStore(NoSql.AutoCreateAndInitializeEventStore());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore()
        => SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore(NoSql.AutoCreateAndInitializeEventStore());

    [TestMethod]
    public override Task EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations()
        => EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(NoSql.AutoCreateAndInitializeEventStore());
}