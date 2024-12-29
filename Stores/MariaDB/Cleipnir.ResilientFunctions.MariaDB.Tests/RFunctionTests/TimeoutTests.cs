﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests.RFunctionTests;

[TestClass]
public class TimeoutTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.TimeoutTests
{
    [TestMethod]
    public override Task ExpiredTimeoutIsAddedToMessages()
        => ExpiredTimeoutIsAddedToMessages(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ExpiredTimeoutMakesReactiveChainThrowTimeoutException()
        => ExpiredTimeoutMakesReactiveChainThrowTimeoutException(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task RegisteredTimeoutIsCancelledAfterReactiveChainCompletes()
        => RegisteredTimeoutIsCancelledAfterReactiveChainCompletes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PendingTimeoutCanBeRemovedFromControlPanel()
        => PendingTimeoutCanBeRemovedFromControlPanel(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PendingTimeoutCanBeUpdatedFromControlPanel()
        => PendingTimeoutCanBeUpdatedFromControlPanel(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExpiredImplicitTimeoutsAreAddedToMessages()
        => ExpiredImplicitTimeoutsAreAddedToMessages(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task TimeoutsWithSameIdsButDifferentContextsDoNotCollide()
        => TimeoutsWithSameIdsButDifferentContextsDoNotCollide(FunctionStoreFactory.Create());
}