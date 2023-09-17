using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class EventSourcesTests : TestTemplates.EventSourcesTests
{
    [TestMethod]
    public override Task EventSourcesSunshineScenario() 
        => EventSourcesSunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEventsShouldBeSameAsAllAfterEmit()
        => ExistingEventsShouldBeSameAsAllAfterEmit(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnored()
        => SecondEventWithExistingIdempotencyKeyIsIgnored(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully()
        => EventSourceBulkMethodOverloadAppendsAllEventsSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventSourcesSunshineScenarioUsingEventStore()
        => EventSourcesSunshineScenarioUsingEventStore(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore()
        => SecondEventWithExistingIdempotencyKeyIsIgnoredUsingEventStore(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations()
        => EventSourceRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(
            FunctionStoreFactory.Create()
        );
}