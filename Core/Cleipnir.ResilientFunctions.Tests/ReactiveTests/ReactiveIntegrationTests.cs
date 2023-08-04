using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive;
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
        var rFunctions = new RFunctions(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var rAction = rFunctions.RegisterAction<string>(
            functionTypeId,
            inner: async (_, context) =>
            {
                var es = await context.EventSource;
                await es.SuspendFor(timeoutEventId: "timeout", resumeAfter: TimeSpan.FromSeconds(1));
            });
        
        await Should.ThrowAsync<FunctionInvocationSuspendedException>(rAction.Invoke(functionInstanceId.Value, "param"));

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
    }
    
    [TestMethod]
    public async Task EventsAreNotPulledFromEventStoreWhenThereAreNoActiveSubscriptions()
    {
        var store = new InMemoryFunctionStoreTestStub();
        var rFunctions = new RFunctions(store, new Settings(eventSourcePullFrequency: TimeSpan.FromMilliseconds(10)));
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var rAction = rFunctions.RegisterAction<string>(
            functionTypeId,
            inner: async (_, context) =>
            {
                var es = await context.EventSource;
                store.EventSubscriptionPulls.ShouldBe(1);
                await Task.Delay(100);
                store.EventSubscriptionPulls.ShouldBe(1);
                var __ = es.Next();
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
        
        public override async Task<EventsSubscription> SubscribeToEvents(FunctionId functionId)
        {
            await Task.CompletedTask;

            return new EventsSubscription(
                async () =>
                {
                    await Task.CompletedTask;
                    Interlocked.Increment(ref EventSubscriptionPulls);
                    return new List<StoredEvent>();
                }, () =>
                {
                    IsDisposed = true;
                    return ValueTask.CompletedTask;
                }
            );
        }
    }
}