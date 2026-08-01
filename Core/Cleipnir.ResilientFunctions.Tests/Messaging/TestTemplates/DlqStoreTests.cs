using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class DlqStoreTests
{
    public abstract Task AppendedDlqMessagesCanBeFetchedAgain();
    protected async Task AppendedDlqMessagesCanBeFetchedAgain(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var dlqStore = functionStore.DlqStore;
        var storedId = TestStoredId.Create();

        var msg1 = CreateMessage("hello world", position: 123, idempotencyKey: "idempotencyKey1", sender: "sender1", receiver: "receiver1");
        var msg2 = CreateMessage("hello universe", position: 124);

        await dlqStore.Append([msg1.ToStoredIdAndMessage(storedId), msg2.ToStoredIdAndMessage(storedId)]);

        var messages = await dlqStore.GetMessages();
        messages.Count.ShouldBe(2);

        messages[0].StoredId.ShouldBe(storedId);
        messages[0].DefaultDeserialize().ShouldBe("hello world");
        messages[0].Position.ShouldNotBe(123); //the incoming position is not persisted - the dlq position takes its place
        messages[0].IdempotencyKey.ShouldBe("idempotencyKey1");
        messages[0].Sender.ShouldBe("sender1");
        messages[0].Receiver.ShouldBe("receiver1");

        messages[1].StoredId.ShouldBe(storedId);
        messages[1].DefaultDeserialize().ShouldBe("hello universe");
        messages[1].IdempotencyKey.ShouldBeNull();
        messages[1].Sender.ShouldBeNull();
        messages[1].Receiver.ShouldBeNull();

        (messages[0].Position < messages[1].Position).ShouldBeTrue();
    }

    public abstract Task MessagesForProvidedStoredIdsAreFetched();
    protected async Task MessagesForProvidedStoredIdsAreFetched(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var dlqStore = functionStore.DlqStore;
        var storedId1 = TestStoredId.Create();
        var storedId2 = TestStoredId.Create();
        var storedId3 = TestStoredId.Create();

        await dlqStore.Append([
            CreateMessage("msg1", position: 1).ToStoredIdAndMessage(storedId1),
            CreateMessage("msg2", position: 2).ToStoredIdAndMessage(storedId2),
            CreateMessage("msg3", position: 3).ToStoredIdAndMessage(storedId3),
            CreateMessage("msg4", position: 4).ToStoredIdAndMessage(storedId1)
        ]);

        var messages = await dlqStore.GetMessages([storedId1, storedId3]);
        messages.Count.ShouldBe(3);
        messages.Select(m => m.StoredId).ShouldNotContain(storedId2);

        var storedId1Messages = messages.Where(m => m.StoredId == storedId1).ToList();
        storedId1Messages.Count.ShouldBe(2);
        storedId1Messages[0].DefaultDeserialize().ShouldBe("msg1");
        storedId1Messages[1].DefaultDeserialize().ShouldBe("msg4");
        (storedId1Messages[0].Position < storedId1Messages[1].Position).ShouldBeTrue();

        messages.Single(m => m.StoredId == storedId3).DefaultDeserialize().ShouldBe("msg3");
    }

    public abstract Task DeletedDlqMessagesAreRemoved();
    protected async Task DeletedDlqMessagesAreRemoved(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var dlqStore = functionStore.DlqStore;
        var storedId = TestStoredId.Create();

        await dlqStore.Append([
            CreateMessage("msg1", position: 1).ToStoredIdAndMessage(storedId),
            CreateMessage("msg2", position: 2).ToStoredIdAndMessage(storedId),
            CreateMessage("msg3", position: 3).ToStoredIdAndMessage(storedId)
        ]);

        var messages = await dlqStore.GetMessages();
        messages.Count.ShouldBe(3);

        await dlqStore.Delete([messages[0].Position, messages[2].Position]);

        var remaining = await dlqStore.GetMessages();
        remaining.Count.ShouldBe(1);
        remaining.Single().Position.ShouldBe(messages[1].Position);
        remaining.Single().DefaultDeserialize().ShouldBe("msg2");
    }

    public abstract Task FetchingEmptyDeadLetterQueueReturnsEmptyList();
    protected async Task FetchingEmptyDeadLetterQueueReturnsEmptyList(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var dlqStore = functionStore.DlqStore;

        (await dlqStore.GetMessages()).ShouldBeEmpty();
        (await dlqStore.GetMessages([TestStoredId.Create()])).ShouldBeEmpty();
        (await dlqStore.GetMessages([])).ShouldBeEmpty();
    }

    public abstract Task DeletingEmptyOrUnknownPositionsDoesNotThrow();
    protected async Task DeletingEmptyOrUnknownPositionsDoesNotThrow(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var dlqStore = functionStore.DlqStore;
        var storedId = TestStoredId.Create();

        await dlqStore.Append([CreateMessage("msg1", position: 1).ToStoredIdAndMessage(storedId)]);

        await dlqStore.Delete([]);
        await dlqStore.Delete([999_999]);

        (await dlqStore.GetMessages()).Count.ShouldBe(1);
    }

    private static StoredMessage CreateMessage(string content, long position, string? idempotencyKey = null, string? sender = null, string? receiver = null)
        => new(
            content.ToJson().ToUtf8Bytes(),
            content.GetType().SimpleQualifiedName().ToUtf8Bytes(),
            position,
            ReplicaId.Empty,
            idempotencyKey,
            sender,
            receiver
        );
}
