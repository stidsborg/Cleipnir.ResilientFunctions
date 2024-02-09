using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.ReactiveTests;

[TestClass]
public class ReactiveIntegrationTests
{
    [TestMethod]
    public async Task FunctionCanBeSuspendedForASecondSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var rAction = functionsRegistry.RegisterAction<string>(
            functionTypeId,
            inner: async (_, workflow) =>
            {
                var messages = workflow.Messages;
                await messages.SuspendFor(timeoutEventId: "timeout", resumeAfter: TimeSpan.FromSeconds(1));
            });
        
        await Should.ThrowAsync<FunctionInvocationSuspendedException>(rAction.Invoke(functionInstanceId.Value, "param"));

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
    }
    
    [TestMethod]
    public async Task EventsAreNotPulledFromMessageStoreWhenThereAreNoActiveSubscriptions()
    {
        var store = new InMemoryFunctionStoreTestStub();
        var functionsRegistry = new FunctionsRegistry(store, new Settings(messagesPullFrequency: TimeSpan.FromMilliseconds(10)));
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var rAction = functionsRegistry.RegisterAction<string>(
            functionTypeId,
            inner: async (_, workflow) =>
            {
                var messages = workflow.Messages;
                store.EventSubscriptionPulls.ShouldBe(0);
                var __ = messages.First();
                await Task.Delay(100);
                store.EventSubscriptionPulls.ShouldBeGreaterThanOrEqualTo(1);
            });

        await rAction.Invoke(functionInstanceId.Value, "param");
        
        store.IsDisposed.ShouldBeTrue();
    }

    private class InMemoryFunctionStoreTestStub : InMemoryFunctionStore
    {
        public int EventSubscriptionPulls = 0;
        public volatile bool IsDisposed = false;
        
        public override MessagesSubscription SubscribeToMessages(FunctionId functionId)
        {
            return new MessagesSubscription(
                async () =>
                {
                    await Task.CompletedTask;
                    Interlocked.Increment(ref EventSubscriptionPulls);
                    return new List<StoredMessage>();
                }, () =>
                {
                    IsDisposed = true;
                    return ValueTask.CompletedTask;
                }
            );
        }
    }
}