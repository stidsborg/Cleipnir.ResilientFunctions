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
        
        await Should.ThrowAsync<FunctionInvocationSuspendedException>(rAction.Invoke(flowInstance.Value, "param"));

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
    }
}