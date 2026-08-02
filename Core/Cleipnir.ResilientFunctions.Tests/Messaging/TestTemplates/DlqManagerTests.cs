using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

/// <summary>
/// Tests of the <see cref="DlqManager"/> facade obtained from <see cref="FunctionsRegistry.DeadLetterQueue"/> -
/// fetching, deleting and redriving dead lettered messages. What causes a message to be dead lettered
/// (undeserializable payloads, unregistered flow types) is covered by <see cref="MessagesSubscriptionTests"/>.
/// </summary>
public abstract class DlqManagerTests
{
    public abstract Task DeadLetteredMessagesCanBeRedriven();
    protected async Task DeadLetteredMessagesCanBeRedriven(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var flowType = nameof(DeadLetteredMessagesCanBeRedriven).ToFlowType();
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            inner: async Task<string> (string _, Workflow workflow) => await workflow.Message<string>()
        );

        var scheduledByPosition = await rFunc.Schedule("byPosition", "");
        var scheduledByStoredId = await rFunc.Schedule("byStoredId", "");
        var scheduledByFlowId = await rFunc.Schedule("byFlowId", "");
        var byPositionId = rFunc.MapToStoredId("byPosition".ToFlowInstance());
        var byStoredIdId = rFunc.MapToStoredId("byStoredId".ToFlowInstance());
        var byFlowIdId = rFunc.MapToStoredId("byFlowId".ToFlowInstance());
        var untouchedId = rFunc.MapToStoredId("untouched".ToFlowInstance());

        await functionStore.DlqStore.Append([
            CreateMessage(byPositionId, "byPositionMsg"),
            CreateMessage(byStoredIdId, "byStoredIdMsg"),
            CreateMessage(byFlowIdId, "byFlowIdMsg"),
            CreateMessage(untouchedId, "untouchedMsg")
        ]);

        var dlq = functionsRegistry.DeadLetterQueue;
        var byPositionMessage = (await dlq.GetMessages([byPositionId])).Single();

        // Redriven messages must be delivered to their waiting flows, completing them.
        await dlq.Redrive([byPositionMessage.Position]);
        await dlq.Redrive([byStoredIdId]);
        await dlq.Redrive([new FlowId(flowType, "byFlowId")]);

        (await scheduledByPosition.Completion(timeout: TimeSpan.FromSeconds(10))).ShouldBe("byPositionMsg");
        (await scheduledByStoredId.Completion(timeout: TimeSpan.FromSeconds(10))).ShouldBe("byStoredIdMsg");
        (await scheduledByFlowId.Completion(timeout: TimeSpan.FromSeconds(10))).ShouldBe("byFlowIdMsg");

        // Only the redriven messages leave the dead letter queue.
        (await dlq.GetMessages([byPositionId, byStoredIdId, byFlowIdId])).ShouldBeEmpty();
        (await dlq.GetMessages([untouchedId])).Single().DefaultDeserialize().ShouldBe("untouchedMsg");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task AllDeadLetteredMessagesForFlowAreRedriven();
    protected async Task AllDeadLetteredMessagesForFlowAreRedriven(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(AllDeadLetteredMessagesForFlowAreRedriven),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var first = await workflow.Message<string>();
                var second = await workflow.Message<string>();
                return $"{first}|{second}";
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var storedId = rFunc.MapToStoredId("instanceId".ToFlowInstance());

        await functionStore.DlqStore.Append([
            CreateMessage(storedId, "first"),
            CreateMessage(storedId, "second")
        ]);

        var dlq = functionsRegistry.DeadLetterQueue;
        (await dlq.GetMessages([storedId])).Count.ShouldBe(2);

        await dlq.Redrive([storedId]);

        (await scheduled.Completion(timeout: TimeSpan.FromSeconds(10))).ShouldBe("first|second");
        (await dlq.GetMessages()).ShouldBeEmpty();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task RedrivenMessageRetainsIdempotencyKeySenderAndReceiver();
    protected async Task RedrivenMessageRetainsIdempotencyKeySenderAndReceiver(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        // The flow type is not registered on this replica, so the redriven message is held by the message
        // watchdog (rather than delivered and deleted) for the - default and thus long - grace period. That
        // leaves the appended row in place for inspection.
        var storedId = TestStoredId.Create();
        await functionStore.DlqStore.Append([
            CreateMessage(storedId, "hello world", idempotencyKey: "idempotencyKey1", sender: "sender1", receiver: "receiver1")
        ]);

        var dlq = functionsRegistry.DeadLetterQueue;
        await dlq.Redrive([storedId]);

        var messages = await functionStore.MessageStore.GetMessages(storedId);
        messages.Count.ShouldBe(1);
        var message = messages.Single();
        message.DefaultDeserialize().ShouldBe("hello world");
        message.IdempotencyKey.ShouldBe("idempotencyKey1");
        message.Sender.ShouldBe("sender1");
        message.Receiver.ShouldBe("receiver1");

        // The message is stamped with the flow's responsible replica - in a single-replica cluster this one.
        message.Replica.ShouldBe(functionsRegistry.ClusterInfo.ReplicaId);

        (await dlq.GetMessages()).ShouldBeEmpty();
    }

    public abstract Task DeletedDeadLetteredMessagesAreNotRedelivered();
    protected async Task DeletedDeadLetteredMessagesAreNotRedelivered(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(DeletedDeadLetteredMessagesAreNotRedelivered),
            inner: async Task<string> (string _, Workflow workflow) => await workflow.Message<string>()
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var storedId = rFunc.MapToStoredId("instanceId".ToFlowInstance());
        var otherId = rFunc.MapToStoredId("otherInstance".ToFlowInstance());

        await functionStore.DlqStore.Append([
            CreateMessage(storedId, "deleted"),
            CreateMessage(otherId, "kept")
        ]);

        var dlq = functionsRegistry.DeadLetterQueue;
        var deleted = (await dlq.GetMessages([storedId])).Single();
        await dlq.Delete([deleted.Position]);

        // Unlike redrive, delete must not put the message back into the message store - the flow stays waiting.
        (await dlq.GetMessages([storedId])).ShouldBeEmpty();
        (await dlq.GetMessages([otherId])).Single().DefaultDeserialize().ShouldBe("kept");
        (await functionStore.MessageStore.GetMessages(storedId)).ShouldBeEmpty();
        await Should.ThrowAsync<TimeoutException>(() => scheduled.Completion(timeout: TimeSpan.FromSeconds(1)));

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task DeadLetteredMessagesCanBePagedThrough();
    protected async Task DeadLetteredMessagesCanBePagedThrough(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var storedId = TestStoredId.Create();
        await functionStore.DlqStore.Append(
            Enumerable
                .Range(0, 5)
                .Select(i => CreateMessage(storedId, $"msg{i}"))
                .ToList()
        );

        var dlq = functionsRegistry.DeadLetterQueue;
        (await dlq.GetMessages()).Count.ShouldBe(5);

        var firstPage = await dlq.GetMessages(limit: 2);
        firstPage.Count.ShouldBe(2);
        firstPage[0].DefaultDeserialize().ShouldBe("msg0");
        firstPage[1].DefaultDeserialize().ShouldBe("msg1");

        //the offset is exclusive - paging is done by passing the last returned position as the next offset
        var secondPage = await dlq.GetMessages(offset: firstPage[1].Position, limit: 2);
        secondPage.Count.ShouldBe(2);
        secondPage[0].DefaultDeserialize().ShouldBe("msg2");
        secondPage[1].DefaultDeserialize().ShouldBe("msg3");

        var thirdPage = await dlq.GetMessages(offset: secondPage[1].Position, limit: 2);
        thirdPage.Count.ShouldBe(1);
        thirdPage[0].DefaultDeserialize().ShouldBe("msg4");

        (await dlq.GetMessages(offset: thirdPage[0].Position)).ShouldBeEmpty();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task RedrivingAndDeletingUnknownOrEmptyInputIsANoOp();
    protected async Task RedrivingAndDeletingUnknownOrEmptyInputIsANoOp(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var storedId = TestStoredId.Create();
        await functionStore.DlqStore.Append([CreateMessage(storedId, "untouched")]);

        var dlq = functionsRegistry.DeadLetterQueue;

        await dlq.Redrive(new List<long>());
        await dlq.Redrive(new List<StoredId>());
        await dlq.Redrive(new List<FlowId>());
        await dlq.Redrive([999_999L]);
        await dlq.Redrive([TestStoredId.Create()]);
        await dlq.Redrive([new FlowId("UnknownFlowType", "unknownInstance")]);

        await dlq.Delete(new List<long>());
        await dlq.Delete([999_999L]);

        (await dlq.GetMessages()).Single().DefaultDeserialize().ShouldBe("untouched");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    private static StoredMessage CreateMessage(StoredId storedId, string content, string? idempotencyKey = null, string? sender = null, string? receiver = null)
        => new(
            storedId,
            content.ToJson().ToUtf8Bytes(),
            typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            Position: 0,
            Replica: ReplicaId.Empty,
            idempotencyKey,
            sender,
            receiver
        );
}
