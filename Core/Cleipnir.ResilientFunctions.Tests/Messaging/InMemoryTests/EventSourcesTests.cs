using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class MessagesTests : TestTemplates.MessagesTests
{
    [TestMethod]
    public override Task MessagesSunshineScenario() 
        => MessagesSunshineScenario(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout()
        => MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst()
        => MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond()
        => MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task QueueClientReturnsNullAfterTimeout()
        => QueueClientReturnsNullAfterTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnored()
        => SecondEventWithExistingIdempotencyKeyIsIgnored(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task QueueClientCanPullMultipleMessages()
        => QueueClientCanPullMultipleMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task BatchedMessagesIsDeliveredToAwaitingFlows()
        => BatchedMessagesIsDeliveredToAwaitingFlows(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MultipleMessagesCanBeAppendedOneAfterTheOther()
        => MultipleMessagesCanBeAppendedOneAfterTheOther(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PingPongMessagesCanBeExchangedMultipleTimes()
        => PingPongMessagesCanBeExchangedMultipleTimes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SenderIsPersistedAndCanBeFetched()
        => SenderIsPersistedAndCanBeFetched(FunctionStoreFactory.Create());
}