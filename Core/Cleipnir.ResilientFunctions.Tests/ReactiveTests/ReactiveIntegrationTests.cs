using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
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
                await es.SuspendFor(TimeSpan.FromSeconds(1), "timeout");
            });
        
        await Should.ThrowAsync<FunctionInvocationSuspendedException>(rAction.Invoke(functionInstanceId.Value, "param"));

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
    }
}