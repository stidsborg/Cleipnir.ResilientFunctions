﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.InMemoryTests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class MessagesTests : TestTemplates.MessagesTests
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
        => SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations()
        => MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(
            FunctionStoreFactory.Create()
        );

    [TestMethod]
    public override Task BatchedMessagesIsDeliveredToAwaitingFlows()
        => BatchedMessagesIsDeliveredToAwaitingFlows(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MultipleMessagesCanBeAppendedOneAfterTheOther()
        => MultipleMessagesCanBeAppendedOneAfterTheOther(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task PingPongMessagesCanBeExchangedMultipleTimes()
        => PingPongMessagesCanBeExchangedMultipleTimes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NoOpMessageIsIgnored()
        => NoOpMessageIsIgnored(FunctionStoreFactory.Create());
}