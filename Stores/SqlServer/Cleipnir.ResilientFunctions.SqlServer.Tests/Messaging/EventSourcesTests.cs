using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.Messaging;

[TestClass]
public class EventSourcesTests : Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates.EventSourcesTests
{
    [TestMethod]
    public override Task EventSourcesSunshineScenario() 
        => EventSourcesSunshineScenario(Sql.CreateAndInitializeEventStore());

    [TestMethod]
    public override Task ExistingEventsShouldBeSameAsAllAfterEmit()
        => ExistingEventsShouldBeSameAsAllAfterEmit(Sql.CreateAndInitializeEventStore());

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
    
    [TestMethod]
    public override Task EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations()
        => EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(Sql.CreateAndInitializeEventStore());
}