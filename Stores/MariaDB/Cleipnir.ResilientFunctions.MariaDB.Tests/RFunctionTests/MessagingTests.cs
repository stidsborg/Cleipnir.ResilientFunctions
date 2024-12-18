﻿using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests.RFunctionTests;

[TestClass]
public class MessagingTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.MessagingTests
{
    [TestMethod]
    public override Task FunctionCompletesAfterAwaitedMessageIsReceived()
        => FunctionCompletesAfterAwaitedMessageIsReceived(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist()
        => FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task TimeoutEventCausesSuspendedFunctionToBeReInvoked()
        => TimeoutEventCausesSuspendedFunctionToBeReInvoked(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScheduleInvocationWithPublishResultToSpecifiedFunctionId()
        => ScheduleInvocationWithPublishResultToSpecifiedFunctionId(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task IsWorkflowRunningSubscriptionPropertyTurnsFalseAfterWorkflowInvocationHasCompleted()
        => IsWorkflowRunningSubscriptionPropertyTurnsFalseAfterWorkflowInvocationHasCompleted(FunctionStoreFactory.Create());
}