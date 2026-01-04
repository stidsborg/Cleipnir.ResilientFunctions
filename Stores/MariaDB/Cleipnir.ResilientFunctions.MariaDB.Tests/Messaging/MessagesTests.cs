namespace Cleipnir.ResilientFunctions.MariaDb.Tests.Messaging;

[TestClass]
public class MessagesTests : ResilientFunctions.Tests.Messaging.TestTemplates.MessagesTests
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
    public override Task NoOpMessageIsIgnored()
        => NoOpMessageIsIgnored(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task PingPongMessagesCanBeExchangedMultipleTimes()
        => PingPongMessagesCanBeExchangedMultipleTimes(FunctionStoreFactory.Create());
}