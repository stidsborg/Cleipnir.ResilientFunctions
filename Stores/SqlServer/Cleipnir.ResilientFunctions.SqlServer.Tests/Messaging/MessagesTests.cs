﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.Messaging;

[TestClass]
public class MessagesTests : Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates.MessagesTests
{
    [TestMethod]
    public override Task MessagesSunshineScenario() 
        => MessagesSunshineScenario(FunctionStoreFactory.Create());
        
    [TestMethod]
    public override Task MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout()
        => MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst()
        => MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond()
        => MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingEventsShouldBeSameAsAllAfterEmit()
        => ExistingEventsShouldBeSameAsAllAfterEmit(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnored()
        => SecondEventWithExistingIdempotencyKeyIsIgnored(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MessagesBulkMethodOverloadAppendsAllEventsSuccessfully()
        => MessagesBulkMethodOverloadAppendsAllEventsSuccessfully(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task MessagessSunshineScenarioUsingMessageStore()
        => MessagessSunshineScenarioUsingMessageStore(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore()
        => SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations()
        => MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task BatchedMessagesIsDeliveredToAwaitingFlows()
        => BatchedMessagesIsDeliveredToAwaitingFlows(FunctionStoreFactory.Create());
}