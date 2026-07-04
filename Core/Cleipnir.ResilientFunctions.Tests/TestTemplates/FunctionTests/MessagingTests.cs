using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class MessagingTests
{
    public abstract Task FunctionCompletesAfterAwaitedMessageIsReceived();
    public async Task FunctionCompletesAfterAwaitedMessageIsReceived(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rAction = functionsRegistry.RegisterFunc(
            nameof(FunctionCompletesAfterAwaitedMessageIsReceived),
            inner: async Task<string> (string _, Workflow workflow) => await workflow.Message<string>());

        await rAction.Schedule("instanceId", "");
        
        var controlPanel = await rAction.ControlPanel("instanceId");
        controlPanel.ShouldNotBeNull();

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Suspended);
        
        var messagesWriter = rAction.MessageWriters.For("instanceId".ToFlowInstance());
        await messagesWriter.AppendMessage("hello world");

        await controlPanel.WaitForCompletion(allowPostponeAndSuspended: true);
        await controlPanel.Refresh();
        
        controlPanel.Result.ShouldBe("hello world");
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist();
    public async Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var functionId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var rAction = functionsRegistry.RegisterFunc(
            functionId.Type,
            inner: async Task<string> (string _, Workflow workflow)
                => await workflow.Message<string>()
        );

        await Should.ThrowAsync<InvocationSuspendedException>(() =>
            rAction.Run(functionId.Instance.Value, "")
        );
        var sf = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ScheduleInvocationWithPublishResultToSpecifiedFunctionId();
    public async Task ScheduleInvocationWithPublishResultToSpecifiedFunctionId(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var parentFunctionId = TestFlowId.Create();
        var childFunctionId = TestFlowId.Create();
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        FuncRegistration<string, string>? parent = null;
        
        var child = functionsRegistry.RegisterAction(
            childFunctionId.Type,
            inner: Task (string _) => parent!
                .MessageWriters
                .For(parentFunctionId.Instance)
                .AppendMessage("hello world")
        );

        parent = functionsRegistry.RegisterFunc(
            parentFunctionId.Type,
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                await child.Schedule(childFunctionId.Instance.Value, param: "stuff");
                return await workflow.Message<string>();
            }
        );

        await parent.Schedule(parentFunctionId.Instance.Value, "");
        
        var controlPanel = await parent.ControlPanel(parentFunctionId.Instance);
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });
        
        controlPanel.Result.ShouldNotBeNull();
        var functionCompletion = controlPanel.Result;
        functionCompletion.ShouldBe("hello world");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task EmptyMessagesRestartSuspendedFlowsWithoutDeliveryAndAreRemovedAfterwards();
    public async Task EmptyMessagesRestartSuspendedFlowsWithoutDeliveryAndAreRemovedAfterwards(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var flowType = TestFlowId.Create().Type;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch, messagesPullFrequency: TimeSpan.FromMilliseconds(100))
        );

        var invocations = new SyncedCounter();
        var registration = functionsRegistry.RegisterParamless(
            flowType,
            inner: async Task (workflow) =>
            {
                invocations.Increment();
                await workflow.Message<string>();
            }
        );

        await registration.Schedule("instance1");
        await registration.Schedule("instance2");

        var controlPanel1 = await registration.ControlPanel("instance1");
        var controlPanel2 = await registration.ControlPanel("instance2");
        controlPanel1.ShouldNotBeNull();
        controlPanel2.ShouldNotBeNull();
        await controlPanel1.BusyWaitUntil(c => c.Status == Status.Suspended);
        await controlPanel2.BusyWaitUntil(c => c.Status == Status.Suspended);
        invocations.Current.ShouldBe(2);

        var storedId1 = registration.MapToStoredId("instance1".ToFlowInstance());
        var storedId2 = registration.MapToStoredId("instance2".ToFlowInstance());
        var replicaId = functionsRegistry.ClusterInfo.ReplicaId;
        await store.MessageStore.AppendMessages([
            new StoredIdAndMessage(storedId1, StoredMessage.CreateEmpty(replicaId)),
            new StoredIdAndMessage(storedId2, StoredMessage.CreateEmpty(replicaId))
        ]);

        // The empty messages restart both flows without being delivered...
        await BusyWait.Until(() => invocations.Current == 4);

        // ...so both flows suspend on the awaited message again...
        await controlPanel1.BusyWaitUntil(c => c.Status == Status.Suspended);
        await controlPanel2.BusyWaitUntil(c => c.Status == Status.Suspended);

        // ...and the empty messages are deleted from the store once the restarts have happened.
        await BusyWait.Until(async () =>
            (await store.MessageStore.GetMessages(storedId1)).Count == 0 &&
            (await store.MessageStore.GetMessages(storedId2)).Count == 0
        );

        invocations.Current.ShouldBe(4);
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task EmptyMessageIsNotDeliveredToRestartedFlowWhileNonEmptyMessageIs();
    public async Task EmptyMessageIsNotDeliveredToRestartedFlowWhileNonEmptyMessageIs(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var flowId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch, messagesPullFrequency: TimeSpan.FromMilliseconds(100))
        );

        var registration = functionsRegistry.RegisterFunc(
            flowId.Type,
            inner: async Task<string> (string _, Workflow workflow) => await workflow.Message<string>()
        );

        await registration.Schedule(flowId.Instance.Value, "");

        var controlPanel = await registration.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.BusyWaitUntil(c => c.Status == Status.Suspended);

        var storedId = registration.MapToStoredId(flowId.Instance);
        var replicaId = functionsRegistry.ClusterInfo.ReplicaId;
        var serializer = DefaultSerializer.Instance;
        await store.MessageStore.AppendMessages([
            new StoredIdAndMessage(storedId, StoredMessage.CreateEmpty(replicaId)),
            new StoredIdAndMessage(
                storedId,
                new StoredMessage(
                    serializer.Serialize("hello world", typeof(string)),
                    serializer.SerializeType(typeof(string)),
                    Position: 0,
                    Replica: replicaId
                )
            )
        ]);

        // The batch restarts the flow with only the non-empty message delivered.
        await controlPanel.WaitForCompletion(allowPostponeAndSuspended: true);
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("hello world");

        // Both the delivered and the empty message are eventually deleted from the store.
        await BusyWait.Until(async () => (await store.MessageStore.GetMessages(storedId)).Count == 0);

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task EmptyMessageIsNotDeliveredWhenFlowIsRestartedViaControlPanel();
    public async Task EmptyMessageIsNotDeliveredWhenFlowIsRestartedViaControlPanel(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var flowId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        // Watchdogs are disabled so the pending messages can only reach the flow via the control panel restart's
        // hand-over (the RestartExecution route) - not via the MessageWatchdog route.
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch, enableWatchdogs: false)
        );

        var registration = functionsRegistry.RegisterFunc(
            flowId.Type,
            inner: async Task<string> (string _, Workflow workflow) => await workflow.Message<string>()
        );

        await registration.Schedule(flowId.Instance.Value, "");

        var controlPanel = await registration.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.BusyWaitUntil(c => c.Status == Status.Suspended);

        var storedId = registration.MapToStoredId(flowId.Instance);
        var replicaId = functionsRegistry.ClusterInfo.ReplicaId;
        var serializer = DefaultSerializer.Instance;
        await store.MessageStore.AppendMessages([
            new StoredIdAndMessage(storedId, StoredMessage.CreateEmpty(replicaId)),
            new StoredIdAndMessage(
                storedId,
                new StoredMessage(
                    serializer.Serialize("hello world", typeof(string)),
                    serializer.SerializeType(typeof(string)),
                    Position: 0,
                    Replica: replicaId
                )
            )
        ]);

        await controlPanel.ScheduleRestart();

        // The restarted flow receives only the non-empty message.
        await controlPanel.WaitForCompletion(allowPostponeAndSuspended: true);
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("hello world");

        // The empty message is deleted by the restart hand-over; the delivered one is deleted after delivery.
        await BusyWait.Until(async () => (await store.MessageStore.GetMessages(storedId)).Count == 0);

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}