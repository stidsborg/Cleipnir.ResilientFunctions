using System;
using System.Linq;
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
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch, messagesPullFrequency: TimeSpan.FromMilliseconds(100))
        );

        var awaitMessage = new SyncedFlag();
        var registration = functionsRegistry.RegisterFunc(
            flowId.Type,
            inner: async Task<string> (string _, Workflow workflow) =>
                awaitMessage.IsRaised ? await workflow.Message<string>() : "no message awaited"
        );

        // Complete the flow without it consuming any messages.
        await registration.Run(flowId.Instance.Value, "");

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

        // The pending messages must not resurrect the completed flow.
        await Task.Delay(500);
        var controlPanel = await registration.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("no message awaited");

        // An explicit restart does not pull messages itself - the MessageWatchdog delivers the pending non-empty
        // message to the restarted flow, while the empty message is never delivered.
        awaitMessage.Raise();
        await controlPanel.ScheduleRestart();
        await controlPanel.WaitForCompletion(allowPostponeAndSuspended: true);
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("hello world");

        // Both rows end up deleted: the non-empty message is inlined into the completed flow's effect state (and
        // its row removed), while the empty poke is simply deleted - a completed flow needs no restart.
        await BusyWait.Until(async () => (await store.MessageStore.GetMessages(storedId)).Count == 0);

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task PendingMessageIsDeliveredWhenCompletedFlowIsPostponedAndRestartedByWatchdog();
    public async Task PendingMessageIsDeliveredWhenCompletedFlowIsPostponedAndRestartedByWatchdog(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var flowId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                messagesPullFrequency: TimeSpan.FromMilliseconds(100),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );

        var awaitMessage = new SyncedFlag();
        var registration = functionsRegistry.RegisterFunc(
            flowId.Type,
            inner: async Task<string> (string _, Workflow workflow) =>
                awaitMessage.IsRaised ? await workflow.Message<string>() : "no message awaited"
        );

        // Complete the flow without it consuming any messages.
        await registration.Run(flowId.Instance.Value, "");

        var storedId = registration.MapToStoredId(flowId.Instance);
        var replicaId = functionsRegistry.ClusterInfo.ReplicaId;
        var serializer = DefaultSerializer.Instance;
        await store.MessageStore.AppendMessages([
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

        // The message is inlined into the completed flow's effect state and its row deleted.
        await BusyWait.Until(async () => (await store.MessageStore.GetMessages(storedId)).Count == 0);

        // Resurrect the completed flow via Postpone - the PostponedWatchdog's restart path must also hand the
        // inlined message over (it travels in the effect snapshot, not through any restart-specific channel).
        awaitMessage.Raise();
        var controlPanel = await registration.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Postpone(DateTime.UtcNow);

        await controlPanel.BusyWaitUntil(c => c.Status == Status.Succeeded);
        controlPanel.Result.ShouldBe("hello world");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task PendingMessageIsDeliveredWhenCompletedFlowIsRestartedOnDifferentReplica();
    public async Task PendingMessageIsDeliveredWhenCompletedFlowIsRestartedOnDifferentReplica(Task<IFunctionStore> functionStore)
    {
        var store = await functionStore;

        var flowId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var awaitMessage = new SyncedFlag();

        Func<FunctionsRegistry> createRegistry = () => new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler.Catch, messagesPullFrequency: TimeSpan.FromMilliseconds(100))
        );
        Func<FunctionsRegistry, FuncRegistration<string, string>> register = registry => registry.RegisterFunc(
            flowId.Type,
            inner: async Task<string> (string _, Workflow workflow) =>
                awaitMessage.IsRaised ? await workflow.Message<string>() : "no message awaited"
        );

        using var publisherRegistry = createRegistry();
        var publisherRegistration = register(publisherRegistry);

        // Complete the flow on the publisher replica without it consuming any messages.
        await publisherRegistration.Run(flowId.Instance.Value, "");

        // Append a message stamped with the publisher replica - its watchdog fetches it, finds the flow
        // completed and inlines it into the flow's effect state.
        var storedId = publisherRegistration.MapToStoredId(flowId.Instance);
        var serializer = DefaultSerializer.Instance;
        await store.MessageStore.AppendMessages([
            new StoredIdAndMessage(
                storedId,
                new StoredMessage(
                    serializer.Serialize("hello world", typeof(string)),
                    serializer.SerializeType(typeof(string)),
                    Position: 0,
                    Replica: publisherRegistry.ClusterInfo.ReplicaId
                )
            )
        ]);
        await BusyWait.Until(async () => (await store.MessageStore.GetMessages(storedId)).Count == 0);

        // Restart the flow from a DIFFERENT replica: the inlined message travels in the effect snapshot the
        // restart hands over, so delivery does not depend on the publisher replica's in-memory state.
        using var restartingRegistry = createRegistry();
        var restartingRegistration = register(restartingRegistry);

        awaitMessage.Raise();
        var controlPanel = await restartingRegistration.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.ScheduleRestart();
        await controlPanel.WaitForCompletion(allowPostponeAndSuspended: true);
        await controlPanel.Refresh();
        controlPanel.Status.ShouldBe(Status.Succeeded);
        controlPanel.Result.ShouldBe("hello world");

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}