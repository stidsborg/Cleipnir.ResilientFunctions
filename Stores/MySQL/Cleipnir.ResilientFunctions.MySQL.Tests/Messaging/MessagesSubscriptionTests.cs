﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.Messaging;

[TestClass]
public class MessagesSubscriptionTests : Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates.MessagesSubscriptionTests
{
    [TestMethod]
    public override Task EventsSubscriptionSunshineScenario()
        => EventsSubscriptionSunshineScenario(FunctionStoreFactory.Create());
}