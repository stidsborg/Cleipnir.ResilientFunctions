using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class SemaphoreTests
{
    public abstract Task SunshineTest();
    public async Task SunshineTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;

        var checkSemaphoreFlag = new SyncedFlag();
        var continueFlowFlag = new SyncedFlag();
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                var semaphore = workflow.Semaphores.Create("SomeGroup", "SomeInstance", maximumCount: 1);
                var @lock = await semaphore.Acquire();
                checkSemaphoreFlag.Raise();
                await continueFlowFlag.WaitForRaised();

                await @lock.DisposeAsync();
            });

        var scheduledFlow = await rAction.Schedule(flowInstance, "hello");
        
        await checkSemaphoreFlag.WaitForRaised();
        
        var storedId = rAction.MapToStoredId(flowId);
        var queued = await store.SemaphoreStore.GetQueued("SomeGroup", "SomeInstance", count: 10);
        queued.Count.ShouldBe(1);
        var queuedStoredId = queued.Single();
        queuedStoredId.ShouldBe(storedId);
        
        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        var existingSemaphores = await controlPanel.Semaphores.GetAll();
        existingSemaphores.Count.ShouldBe(1);
        var existingSemaphore = existingSemaphores.Single();
        existingSemaphore.Group.ShouldBe("SomeGroup");
        existingSemaphore.Instance.ShouldBe("SomeInstance");
        
        continueFlowFlag.Raise();
        
        await scheduledFlow.Completion();
        queued = await store.SemaphoreStore.GetQueued("SomeGroup", "SomeInstance", count: 10);
        queued.ShouldBeEmpty();
    }
    
    public abstract Task WaitingFlowIsInterruptedAfterSemaphoreBecomesFree();
    public async Task WaitingFlowIsInterruptedAfterSemaphoreBecomesFree(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId1 = TestFlowId.Create();
        var flowId2 = TestFlowId.Create();

        var checkSemaphoreFlag = new SyncedFlag();
        var continueFlowFlag = new SyncedFlag();
        
        var firstFlow = functionsRegistry.RegisterAction(
            flowId1.Type,
            async Task(string param, Workflow workflow) =>
            {
                var semaphore = workflow.Semaphores.Create("SomeGroup", "SomeInstance", maximumCount: 1);
                var @lock = await semaphore.Acquire();
                checkSemaphoreFlag.Raise();
                await continueFlowFlag.WaitForRaised();

                await @lock.DisposeAsync();
            });
        
        var secondFlow = functionsRegistry.RegisterAction(
            flowId2.Type,
            async Task(string param, Workflow workflow) =>
            {
                var semaphore = workflow.Semaphores.Create("SomeGroup", "SomeInstance", maximumCount: 1);
                var @lock = await semaphore.Acquire();
                checkSemaphoreFlag.Raise();
                await continueFlowFlag.WaitForRaised();

                await @lock.DisposeAsync();
            });
        
       
        await firstFlow.Schedule(flowId1.Instance, "hello");
        await checkSemaphoreFlag.WaitForRaised();
        await secondFlow.Schedule(flowId2.Instance, "hello");
        var controlPanelFlow2 = await secondFlow.ControlPanel(flowId2.Instance).ShouldNotBeNullAsync();

        await BusyWait.Until(async () =>
        {
            await controlPanelFlow2.Refresh();
            return controlPanelFlow2.Status == Status.Suspended;
        });
        
        continueFlowFlag.Raise();
        
        await BusyWait.Until(async () =>
        {
            await controlPanelFlow2.Refresh();
            return controlPanelFlow2.Status == Status.Succeeded;
        });
    }
    
    public abstract Task SemaphoreAllowsTwoFlowsToContinueAtTheSameTime();
    public async Task SemaphoreAllowsTwoFlowsToContinueAtTheSameTime(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId1 = TestFlowId.Create();
        var flowId2 = TestFlowId.Create();
        var flowId3 = TestFlowId.Create();
        
        var firstFlowAcquiredSemaphore = new SyncedFlag();
        var secondFlowAcquiredSemaphore = new SyncedFlag();
        var continueFlowFlag = new SyncedFlag();
        
        var firstFlow = functionsRegistry.RegisterAction(
            flowId1.Type,
            async Task(string param, Workflow workflow) =>
            {
                var semaphore = workflow.Semaphores.Create("SomeGroup", "SomeInstance", maximumCount: 2);
                var @lock = await semaphore.Acquire();
                firstFlowAcquiredSemaphore.Raise();
                await continueFlowFlag.WaitForRaised();

                await @lock.DisposeAsync();
            });
        
        var secondFlow = functionsRegistry.RegisterAction(
            flowId2.Type,
            async Task(string param, Workflow workflow) =>
            {
                var semaphore = workflow.Semaphores.Create("SomeGroup", "SomeInstance", maximumCount: 2);
                var @lock = await semaphore.Acquire();
                secondFlowAcquiredSemaphore.Raise();
                await continueFlowFlag.WaitForRaised();

                await @lock.DisposeAsync();
            });
        
        var thirdFlow = functionsRegistry.RegisterAction(
            flowId3.Type,
            async Task(string param, Workflow workflow) =>
            {
                var semaphore = workflow.Semaphores.Create("SomeGroup", "SomeInstance", maximumCount: 2);
                var @lock = await semaphore.Acquire();
                await @lock.DisposeAsync();
            });
        
        var firstFlowTask = Task.Run(() => firstFlow.Invoke(flowId1.Instance, "hello"));
        var secondFlowTask = Task.Run(() => secondFlow.Invoke(flowId2.Instance, "hello"));

        try
        {
            await firstFlowAcquiredSemaphore.WaitForRaised();
            await secondFlowAcquiredSemaphore.WaitForRaised();
        }
        catch (Exception)
        {
            await firstFlowTask;
            await secondFlowTask;

            throw;
        }

        var scheduled3 = await thirdFlow.Schedule(flowId3.Instance, "hello");
        
        var thirdFlowControlPanel = await thirdFlow.ControlPanel(flowId3.Instance).ShouldNotBeNullAsync();
        await thirdFlowControlPanel.BusyWaitUntil(c => c.Status == Status.Suspended);
        
        var storedId1 = firstFlow.MapToStoredId(flowId1);
        var storedId2 = secondFlow.MapToStoredId(flowId2);
        var storedId3 = thirdFlow.MapToStoredId(flowId3);
        var queued = await store.SemaphoreStore.GetQueued("SomeGroup", "SomeInstance", count: 10);
        queued.Count.ShouldBe(3);
        queued.Any(s => s == storedId1).ShouldBeTrue();
        queued.Any(s => s == storedId2).ShouldBeTrue();
        queued.Any(s => s == storedId3).ShouldBeTrue();
        
        continueFlowFlag.Raise();
        
        await scheduled3.Completion();
        
        queued = await store.SemaphoreStore.GetQueued("SomeGroup", "SomeInstance", count: 10);
        queued.Count.ShouldBe(0);
    }
    
    public abstract Task ExistingSemaphoreCanBeForceReleased();
    public async Task ExistingSemaphoreCanBeForceReleased(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                var semaphore = workflow.Semaphores.Create("SomeGroup", "SomeInstance", maximumCount: 1);
                await semaphore.Acquire();
                throw new SuspendInvocationException();
            });

        var scheduledFlow = await rAction.Schedule(flowInstance, "hello");
        var controlPanel = await rAction.ControlPanel(flowInstance).ShouldNotBeNullAsync();
        await controlPanel.BusyWaitUntil(c => c.Status == Status.Suspended);
        
        var existingSemaphores = await controlPanel.Semaphores.GetAll();
        existingSemaphores.Count.ShouldBe(1);
        var existingSemaphore = existingSemaphores.Single();
        existingSemaphore.Group.ShouldBe("SomeGroup");
        existingSemaphore.Instance.ShouldBe("SomeInstance");

        await controlPanel.Semaphores.ForceRelease(existingSemaphore, maximumCount: 1);
        
        var queued = await store.SemaphoreStore.GetQueued("SomeGroup", "SomeInstance", count: 10);
        queued.ShouldBeEmpty();
    }
}