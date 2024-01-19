using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class MessagessTests : TestTemplates.MessagessTests
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
        => SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations()
        => MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(
            FunctionStoreFactory.Create()
        );
}