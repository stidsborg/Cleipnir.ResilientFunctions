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
}