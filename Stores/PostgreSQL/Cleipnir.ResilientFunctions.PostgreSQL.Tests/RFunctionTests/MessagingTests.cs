﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class MessagingTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.MessagingTests
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