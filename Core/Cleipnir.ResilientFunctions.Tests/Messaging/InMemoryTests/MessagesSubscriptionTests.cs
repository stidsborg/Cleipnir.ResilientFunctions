﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class MessagesSubscriptionTests : TestTemplates.MessagesSuscriptionTests
{
    [TestMethod]
    public override Task EventsSubscriptionSunshineScenario()
        => EventsSubscriptionSunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task EventsWithSameIdempotencyKeyAreFilterOut()
        => EventsWithSameIdempotencyKeyAreFilterOut(FunctionStoreFactory.Create());
}