using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class MessagesSubscriptionTests : TestTemplates.MessagesSubscriptionTests
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

    [TestMethod]
    public override Task QueueClientFilterParameterFiltersMessages()
        => QueueClientFilterParameterFiltersMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task QueueClientWorksWithCustomSerializer()
        => QueueClientWorksWithCustomSerializer(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NoOpMessageIsIgnoredByQueueClient()
        => NoOpMessageIsIgnoredByQueueClient(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task BatchedMessagesAreDeliveredToMultipleFlows()
        => BatchedMessagesAreDeliveredToMultipleFlows(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task QueueClientSupportsMultiFlowMessageExchange()
        => QueueClientSupportsMultiFlowMessageExchange(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task QueueManagerSkipsMessageWithDeserializationError()
        => QueueManagerSkipsMessageWithDeserializationError(FunctionStoreFactory.Create());
}