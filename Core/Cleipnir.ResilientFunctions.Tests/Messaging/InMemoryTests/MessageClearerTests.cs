using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.InMemoryTests;

[TestClass]
public class MessageClearerTests
{
    private static readonly TimeSpan MaxWait = TimeSpan.FromSeconds(10);

    [TestMethod]
    public async Task ClearCoalescesCallsArrivingWhileADeleteIsInFlight()
    {
        var firstDeleteReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstDelete = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var callCount = 0;

        var store = new ControllableMessageStore(async _ =>
        {
            if (Interlocked.Increment(ref callCount) == 1)
            {
                firstDeleteReached.SetResult();
                await releaseFirstDelete.Task;
            }
        });
        var clearer = CreateClearer(store);

        // First call starts the drain and blocks inside DeleteMessages.
        var first = clearer.Clear([1]);
        await firstDeleteReached.Task.WaitAsync(MaxWait);

        // These arrive mid-flight, so they should be batched into a single follow-up delete.
        var second = clearer.Clear([2]);
        var third = clearer.Clear([3]);

        releaseFirstDelete.SetResult();
        await Task.WhenAll(first, second, third).WaitAsync(MaxWait);

        store.DeletedBatches.Count.ShouldBe(2);
        store.DeletedBatches[0].ShouldBe(new long[] { 1 });
        store.DeletedBatches[1].OrderBy(p => p).ShouldBe(new long[] { 2, 3 });
    }

    [TestMethod]
    public async Task ClearRetriesUntilDeleteSucceedsAndNotifiesEachFailure()
    {
        var unhandledLock = new Lock();
        var unhandled = new List<FrameworkException>();
        var failuresRemaining = 3;

        var store = new ControllableMessageStore(_ =>
            Interlocked.Decrement(ref failuresRemaining) >= 0
                ? throw new InvalidOperationException("boom")
                : Task.CompletedTask
        );
        var clearer = CreateClearer(
            store,
            onUnhandledException: e => { lock (unhandledLock) unhandled.Add(e); },
            retryDelay: TimeSpan.FromMilliseconds(10)
        );

        // Despite the first three deletes throwing, the caller's task completes (it is never faulted).
        await clearer.Clear([1]).WaitAsync(MaxWait);

        store.DeletedPositions.ShouldContain(1L);
        lock (unhandledLock)
        {
            unhandled.Count.ShouldBe(3);
            unhandled.ShouldAllBe(e => e.InnerException is InvalidOperationException);
        }
    }

    [TestMethod]
    public async Task ClearCompletesEveryCallerUnderConcurrentLoad()
    {
        var store = new ControllableMessageStore(_ => Task.CompletedTask);
        var clearer = CreateClearer(store);

        var tasks = Enumerable
            .Range(0, 200)
            .Select(i => clearer.Clear([i]))
            .ToArray();

        await Task.WhenAll(tasks).WaitAsync(MaxWait);

        store.DeletedPositions.OrderBy(p => p).ShouldBe(Enumerable.Range(0, 200).Select(i => (long)i));
    }

    [TestMethod]
    public async Task ClearRemovesPositionsFromIgnoreSetOnceDeleted()
    {
        var store = new ControllableMessageStore(_ => Task.CompletedTask);
        var clearer = CreateClearer(store);

        clearer.MarkPushed([1, 2, 3]);
        clearer.NonClearedPositions().OrderBy(p => p).ShouldBe(new long[] { 1, 2, 3 });

        await clearer.Clear([2]).WaitAsync(MaxWait);

        clearer.NonClearedPositions().OrderBy(p => p).ShouldBe(new long[] { 1, 3 });
    }

    [TestMethod]
    public async Task ClearedPositionsAreGoneFromTheStoreWhenTheReturnedTaskCompletes()
    {
        var functionStore = new InMemoryFunctionStore();
        var messageStore = functionStore.MessageStore;
        var storedId = TestStoredId.Create();

        await messageStore.AppendMessages([
            new StoredIdAndMessage(storedId, Message()),
            new StoredIdAndMessage(storedId, Message()),
            new StoredIdAndMessage(storedId, Message())
        ]);
        var positions = (await messageStore.GetMessages(storedId)).Select(m => m.Position).ToList();
        positions.Count.ShouldBe(3);

        var clearer = CreateClearer(messageStore);
        await clearer.Clear(positions.Take(2).ToList()).WaitAsync(MaxWait);

        // The instant Clear's task completes, the cleared messages must already be gone from the store.
        var remaining = (await messageStore.GetMessages(storedId)).Select(m => m.Position).ToList();
        remaining.ShouldBe(new[] { positions[2] });
    }

    private static StoredMessage Message()
        => new(MessageContent: new byte[] { 1 }, MessageType: new byte[] { 2 }, Position: 0, Replica: ReplicaId.Empty);

    private static MessageClearer CreateClearer(
        IMessageStore messageStore,
        Action<FrameworkException>? onUnhandledException = null,
        TimeSpan? retryDelay = null)
        => new(
            messageStore,
            new UnhandledExceptionHandler(onUnhandledException ?? (_ => { })),
            retryDelay ?? TimeSpan.FromSeconds(1)
        );

    // Minimal IMessageStore that only implements the positions-only DeleteMessages (the sole method
    // MessageClearer touches); every other member is irrelevant to these tests.
    private sealed class ControllableMessageStore(Func<IReadOnlyList<long>, Task> onDelete) : IMessageStore
    {
        private readonly Lock _lock = new();
        public List<long[]> DeletedBatches { get; } = new();
        public IEnumerable<long> DeletedPositions => DeletedBatches.SelectMany(b => b);

        public async Task DeleteMessages(IReadOnlyList<long> positions)
        {
            var batch = positions.ToArray();
            lock (_lock)
                DeletedBatches.Add(batch);
            await onDelete(batch);
        }

        public Task Initialize() => throw new NotSupportedException();
        public Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages) => throw new NotSupportedException();
        public Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage) => throw new NotSupportedException();
        public Task DeleteMessages(StoredId storedId, IEnumerable<long> positions) => throw new NotSupportedException();
        public Task Truncate(StoredId storedId) => throw new NotSupportedException();
        public Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId) => throw new NotSupportedException();
        public Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions) => throw new NotSupportedException();
        public Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds) => throw new NotSupportedException();
        public Task<List<StoredMessages>> GetMessagesForReplica(ReplicaId replicaId, IReadOnlyList<long> ignorePositions) => throw new NotSupportedException();
        public Task<List<StoredIdAndPosition>> GetCrashedReplicaMessages(IReadOnlySet<ReplicaId> liveReplicas) => throw new NotSupportedException();
        public Task SetReplica(IEnumerable<long> positions, ReplicaId newReplica, ReplicaId expectedReplica) => throw new NotSupportedException();
    }
}
