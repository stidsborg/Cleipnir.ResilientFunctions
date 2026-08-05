using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class UnawaitedSubflowTests
{
    [TestMethod]
    public async Task FlowReturningSuccessWithExecutingSubflowFailsFast()
    {
        var store = new InMemoryFunctionStore();
        await store.Initialize();
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;

        ActionRegistration<string> rAction = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(
            store,
            r =>
            {
                rAction = r.RegisterAction(
                    flowType,
                    Task (string param, Workflow workflow) =>
                    {
                        _ = workflow.Effect.RunParallelle(async () =>
                        {
                            await Task.Delay(5_000);
                            return 1;
                        });

                        return Task.CompletedTask;
                    });
            }
        );

        await rAction.Schedule(flowInstance.ToString(), "hello");

        var storedId = rAction.MapToStoredId(flowId.Instance);
        await BusyWait.Until(() =>
            store.GetFunction(storedId).SelectAsync(sf => sf?.Status == Status.Failed)
        );

        var storedFlow = await store.GetFunction(storedId);
        storedFlow!.Exception.ShouldNotBeNull();
        storedFlow.Exception.ExceptionMessage.ShouldContain("subflow");
    }
}
