using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.Messaging;

[TestClass]
public class MessagesSubscriptionTests : Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates.MessagesSuscriptionTests
{
    [TestMethod]
    public override Task EventsSubscriptionSunshineScenario()
        => EventsSubscriptionSunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventsWithSameIdempotencyKeyAreFilterOut()
        => EventsWithSameIdempotencyKeyAreFilterOut(FunctionStoreFactory.Create());
}