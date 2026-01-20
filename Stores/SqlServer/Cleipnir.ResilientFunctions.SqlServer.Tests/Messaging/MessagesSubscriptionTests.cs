using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.Messaging;

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

    [TestMethod]
    public override Task RegisteredTimeoutIsRemovedWhenPullingMessage()
        => RegisteredTimeoutIsRemovedWhenPullingMessage(FunctionStoreFactory.Create());
}