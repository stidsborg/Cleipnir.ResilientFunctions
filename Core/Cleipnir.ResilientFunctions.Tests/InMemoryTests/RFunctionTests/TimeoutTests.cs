﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class TimeoutTests : TestTemplates.FunctionTests.TimeoutTests
{
    [TestMethod]
    public override Task ExpiredTimeoutIsAddedToMessages()
        => ExpiredTimeoutIsAddedToMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExpiredTimeoutMakesReactiveChainThrowTimeoutException()
        => ExpiredTimeoutMakesReactiveChainThrowTimeoutException(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExpiredImplicitTimeoutsAreAddedToMessages()
        => ExpiredImplicitTimeoutsAreAddedToMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task TimeoutsWithSameIdsButDifferentContextsDoNotCollide()
        => TimeoutsWithSameIdsButDifferentContextsDoNotCollide(FunctionStoreFactory.Create());
}