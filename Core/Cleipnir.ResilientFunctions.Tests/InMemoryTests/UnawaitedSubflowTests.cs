using System;
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
    public async Task FlowReturningSuccessWithExecutingSubflowWaitsForItBeforeSucceeding()
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
                            await Task.Delay(100);
                            return 1;
                        });

                        return Task.CompletedTask;
                    });
            }
        );

        await rAction.Schedule(flowInstance.ToString(), "hello");

        var storedId = rAction.MapToStoredId(flowId.Instance);
        await BusyWait.Until(() =>
            store.GetFunction(storedId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        //the un-awaited subflow was waited out, so its effect is part of the succeeded flow
        var storedFlow = await store.GetFunction(storedId);
        storedFlow!.Effects!.Count.ShouldBe(1);
    }

    [TestMethod]
    public async Task FlowOutcomeIsNotPersistedUntilExecutingSubflowsHaveDrained()
    {
        var store = new InMemoryFunctionStore();
        await store.Initialize();
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;

        var releaseSubflow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

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
                            await releaseSubflow.Task;
                            return 1;
                        });

                        return Task.CompletedTask;
                    });
            }
        );

        await rAction.Schedule(flowInstance.ToString(), "hello");

        var storedId = rAction.MapToStoredId(flowId.Instance);
        await Task.Delay(250);

        //the invocation has returned but is held in teardown by the still-executing subflow
        var whileDraining = await store.GetFunction(storedId);
        whileDraining!.Status.ShouldBe(Status.Executing);

        releaseSubflow.SetResult();

        await BusyWait.Until(() =>
            store.GetFunction(storedId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        //the drained subflow got to persist its effect before the outcome was written
        var storedFlow = await store.GetFunction(storedId);
        storedFlow!.Effects!.Count.ShouldBe(1);
    }

    [TestMethod]
    public async Task PostponedFlowWaitsForExecutingSubflowBeforeBecomingRestartable()
    {
        var store = new InMemoryFunctionStore();
        await store.Initialize();
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;

        var releaseSubflow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var invocations = 0;

        FuncRegistration<string, string> rFunc = null!;
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(
            store,
            r =>
            {
                rFunc = r.RegisterFunc(
                    flowType,
                    Task<Result<string>> (string param, Workflow workflow) =>
                    {
                        if (invocations++ > 0)
                            return Succeed.WithValue("done").ToTask();

                        _ = workflow.Effect.RunParallelle(async () =>
                        {
                            await releaseSubflow.Task;
                            return 1;
                        });

                        return Postpone.Until(DateTime.UtcNow.AddMilliseconds(100)).ToResult<string>().ToTask();
                    });
            }
        );

        await rFunc.Schedule(flowInstance.ToString(), "hello");

        var storedId = rFunc.MapToStoredId(flowId.Instance);
        await Task.Delay(250);

        //the postponement is held back - persisting it would make the flow restartable while a subflow still runs
        var whileDraining = await store.GetFunction(storedId);
        whileDraining!.Status.ShouldBe(Status.Executing);
        invocations.ShouldBe(1);

        releaseSubflow.SetResult();

        await BusyWait.Until(() =>
            store.GetFunction(storedId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
    }
}
