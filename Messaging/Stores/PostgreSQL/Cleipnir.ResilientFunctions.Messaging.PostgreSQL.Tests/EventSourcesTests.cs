namespace Cleipnir.ResilientFunctions.Messaging.PostgreSQL.Tests;

[TestClass]
public class EventSourcesTests : Messaging.Tests.TestTemplates.EventSourcesTests
{
    [TestMethod]
    public override Task EventSourcesSunshineScenario() 
        => EventSourcesSunshineScenario(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnored()
        => SecondEventWithExistingIdempotencyKeyIsIgnored(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully()
        => EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task EventSourcesSunshineScenarioUsingEventStore()
        => EventSourcesSunshineScenarioUsingEventStore(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore()
        => SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore(Sql.CreateAndInitializeEventStore());
}