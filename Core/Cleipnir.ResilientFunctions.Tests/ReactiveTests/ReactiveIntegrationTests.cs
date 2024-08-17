using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
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
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var rAction = functionsRegistry.RegisterAction<string>(
            flowType,
            inner: async (_, workflow) =>
            {
                var messages = workflow.Messages;
                await messages.SuspendFor(timeoutEventId: "timeout", resumeAfter: TimeSpan.FromSeconds(1));
            });
        
        await Should.ThrowAsync<InvocationSuspendedException>(rAction.Invoke(flowInstance.Value, "param"));

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
    }
    
    [TestMethod]
    public async Task SyncingStopsAfterReactiveChainCompletion()
    {
        var counter = new SyncedCounter();
        
        var source = new TestSource(
            NoOpRegisteredTimeouts.Instance,
            syncStore: _ =>
            {
                counter.Increment();
                return Task.CompletedTask;
            });

        var listTask = source.Take(1).ToList();
        source.SignalNext(1);
        
        await listTask;
        var beforeDelayCounter = counter.Current;
        await Task.Delay(250);
        counter.Current.ShouldBe(beforeDelayCounter);
    }
}