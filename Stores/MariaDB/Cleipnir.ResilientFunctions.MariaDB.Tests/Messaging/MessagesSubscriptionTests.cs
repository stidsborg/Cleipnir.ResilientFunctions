namespace Cleipnir.ResilientFunctions.MariaDb.Tests.Messaging;

[TestClass]
public class MessagesSubscriptionTests : Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates.MessagesSubscriptionTests
{
    [TestMethod]
    public override Task EventsSubscriptionSunshineScenario()
        => EventsSubscriptionSunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task QueueClientCanPullSingleMessage()
        => QueueClientCanPullSingleMessage(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task QueueClientCanPullMultipleMessages()
        => QueueClientCanPullMultipleMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task QueueClientReturnsNullAfterTimeout()
        => QueueClientReturnsNullAfterTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task QueueClientPullsFiveMessagesAndTimesOutOnSixth()
        => QueueClientPullsFiveMessagesAndTimesOutOnSixth(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task OnlyFirstMessageWithSameIdempotencyKeyIsDeliveredAndBothAreRemovedAfterCompletion()
        => OnlyFirstMessageWithSameIdempotencyKeyIsDeliveredAndBothAreRemovedAfterCompletion(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MultipleIterationsWithDuplicateIdempotencyKeysProcessCorrectly()
        => MultipleIterationsWithDuplicateIdempotencyKeysProcessCorrectly(FunctionStoreFactory.Create());
}