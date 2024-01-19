using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.Messaging;

[TestClass]
public class MessagessTests : ResilientFunctions.Tests.Messaging.TestTemplates.MessagessTests
{
    [TestMethod]
    public override Task MessagessSunshineScenario() 
        => MessagessSunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEventsShouldBeSameAsAllAfterEmit()
        => ExistingEventsShouldBeSameAsAllAfterEmit(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnored()
        => SecondEventWithExistingIdempotencyKeyIsIgnored(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesBulkMethodOverloadAppendsAllEventsSuccessfully()
        => MessagesBulkMethodOverloadAppendsAllEventsSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagessSunshineScenarioUsingMessageStore()
        => MessagessSunshineScenarioUsingMessageStore(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore()
        => SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations()
        => MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(FunctionStoreFactory.Create());
}