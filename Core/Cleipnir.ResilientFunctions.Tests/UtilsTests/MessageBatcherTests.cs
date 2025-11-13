using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class MessageBatcherTests
{
    [TestMethod]
    public async Task SingleMessageAppendWritesImmediately()
    {
        // Arrange
        var writtenBatches = new List<List<StoredMessage>>();
        var storedId = new StoredId(Guid.NewGuid());

        var batcher = new MessageBatcher<StoredMessage>(
            (id, messages) =>
            {
                writtenBatches.Add(messages.ToList());
                return Task.CompletedTask;
            }
        );

        var message = CreateMessage("test");

        // Act
        await batcher.Handle(storedId, [message]);

        // Assert
        writtenBatches.Count.ShouldBe(1);
        writtenBatches[0].Count.ShouldBe(1);
        writtenBatches[0][0].MessageContent.ToStringFromUtf8Bytes().ShouldBe("test");
    }

    [TestMethod]
    public async Task ConcurrentAppendsAreBatchedTogether()
    {
        // Arrange
        var writtenBatches = new List<List<StoredMessage>>();
        var firstWriteStartedFlag = new SyncedFlag();
        var allowWriteToComplete = new SyncedFlag();
        var storedId = new StoredId(Guid.NewGuid());
        var writeCount = 0;

        var batcher = new MessageBatcher<StoredMessage>(
            async (id, messages) =>
            {
                writtenBatches.Add(messages.ToList());
                if (Interlocked.Increment(ref writeCount) == 1)
                    firstWriteStartedFlag.Raise();

                await allowWriteToComplete.WaitForRaised();
            }
        );

        // Act
        var task1 = Task.Run(() => batcher.Handle(storedId, [CreateMessage("msg1")]));

        // Wait for first write to start
        await firstWriteStartedFlag.WaitForRaised();

        // Now queue up more messages while first write is in progress
        var task2 = Task.Run(() => batcher.Handle(storedId, [CreateMessage("msg2")]));
        var task3 = Task.Run(() => batcher.Handle(storedId, [CreateMessage("msg3")]));

        // Give tasks time to queue
        await Task.Delay(50);

        // Let the write complete
        allowWriteToComplete.Raise();

        await Task.WhenAll(task1, task2, task3);

        // Assert
        writtenBatches.Count.ShouldBe(2); // First write (msg1), then batched write (msg2, msg3)
        writtenBatches[0].Count.ShouldBe(1);
        writtenBatches[0][0].MessageContent.ToStringFromUtf8Bytes().ShouldBe("msg1");

        writtenBatches[1].Count.ShouldBe(2);
        var batchedMessages = writtenBatches[1].Select(m => m.MessageContent.ToStringFromUtf8Bytes()).ToList();
        batchedMessages.ShouldContain("msg2");
        batchedMessages.ShouldContain("msg3");
    }

    [TestMethod]
    public async Task MultipleMessagesInSingleCallAreWrittenTogether()
    {
        // Arrange
        var writtenBatches = new List<List<StoredMessage>>();
        var storedId = new StoredId(Guid.NewGuid());

        var batcher = new MessageBatcher<StoredMessage>(
            (id, messages) =>
            {
                writtenBatches.Add(messages.ToList());
                return Task.CompletedTask;
            }
        );

        var messages = new[] { CreateMessage("msg1"), CreateMessage("msg2"), CreateMessage("msg3") };

        // Act
        await batcher.Handle(storedId, messages);

        // Assert
        writtenBatches.Count.ShouldBe(1);
        writtenBatches[0].Count.ShouldBe(3);
    }

    [TestMethod]
    public async Task ExceptionDuringWritePropagatestoAllWaiters()
    {
        // Arrange
        var writeCount = 0;
        var writeStartedFlag = new SyncedFlag();
        var allowWriteToCompleteFlag = new SyncedFlag();
        var storedId = new StoredId(Guid.NewGuid());

        var batcher = new MessageBatcher<StoredMessage>(
            async (id, messages) =>
            {
                writeCount++;
                writeStartedFlag.Raise();
                await allowWriteToCompleteFlag.WaitForRaised();
                throw new InvalidOperationException("Write failed!");
            }
        );

        // Act
        var task1 = Task.Run(() => batcher.Handle(storedId, [CreateMessage("msg1")]));

        await writeStartedFlag.WaitForRaised();

        var task2 = Task.Run(() => batcher.Handle(storedId, [CreateMessage("msg2")]));
        var task3 = Task.Run(() => batcher.Handle(storedId, [CreateMessage("msg3")]));

        await Task.Delay(50);
        allowWriteToCompleteFlag.Raise();

        // Assert
        var exception1 = await Should.ThrowAsync<InvalidOperationException>(async () => await task1);
        var exception2 = await Should.ThrowAsync<InvalidOperationException>(async () => await task2);
        var exception3 = await Should.ThrowAsync<InvalidOperationException>(async () => await task3);

        exception1.Message.ShouldBe("Write failed!");
        exception2.Message.ShouldBe("Write failed!");
        exception3.Message.ShouldBe("Write failed!");

        writeCount.ShouldBe(2); // First attempt + retry with accumulated batch
    }

    [TestMethod]
    public async Task AfterExceptionNextCallerBecomesWriterAndSucceeds()
    {
        // Arrange
        var writeCount = 0;
        var writtenBatches = new List<List<StoredMessage>>();
        var storedId = new StoredId(Guid.NewGuid());

        var batcher = new MessageBatcher<StoredMessage>(
            (id, messages) =>
            {
                writeCount++;
                if (writeCount == 1)
                    throw new InvalidOperationException("First write failed!");

                writtenBatches.Add(messages.ToList());
                return Task.CompletedTask;
            }
        );

        // Act
        await Should.ThrowAsync<InvalidOperationException>(
            async () => await batcher.Handle(storedId, [CreateMessage("msg1")])
        );

        // Next call should succeed
        await batcher.Handle(storedId, [CreateMessage("msg2")]);

        // Assert
        writeCount.ShouldBe(2);
        writtenBatches.Count.ShouldBe(1);
        writtenBatches[0][0].MessageContent.ToStringFromUtf8Bytes().ShouldBe("msg2");
    }

    [TestMethod]
    public async Task MultipleBatchesAreDrainedSequentially()
    {
        // Arrange
        var writtenBatches = new List<List<StoredMessage>>();
        var writeDelays = new Queue<SyncedFlag>();
        var flag1 = new SyncedFlag();
        var flag2 = new SyncedFlag();
        var flag3 = new SyncedFlag();
        var storedId = new StoredId(Guid.NewGuid());

        writeDelays.Enqueue(flag1);
        writeDelays.Enqueue(flag2);
        writeDelays.Enqueue(flag3);

        var batcher = new MessageBatcher<StoredMessage>(
            async (id, messages) =>
            {
                writtenBatches.Add(messages.ToList());
                if (writeDelays.Count > 0)
                    await writeDelays.Dequeue().WaitForRaised();
            }
        );

        // Act - Start first write
        var task1 = Task.Run(() => batcher.Handle(storedId, [CreateMessage("msg1")]));
        await Task.Delay(50);

        // Queue more messages
        var task2 = Task.Run(() => batcher.Handle(storedId, [CreateMessage("msg2")]));
        await Task.Delay(50);

        // Complete first write - this should start draining msg2
        flag1.Raise();
        await Task.Delay(50);

        // Queue another while msg2 is being written
        var task3 = Task.Run(() => batcher.Handle(storedId, [CreateMessage("msg3")]));
        await Task.Delay(50);

        // Complete remaining writes
        flag2.Raise();
        await Task.Delay(50);
        flag3.Raise();

        await Task.WhenAll(task1, task2, task3);

        // Assert
        writtenBatches.Count.ShouldBe(3);
        writtenBatches[0][0].MessageContent.ToStringFromUtf8Bytes().ShouldBe("msg1");
        writtenBatches[1][0].MessageContent.ToStringFromUtf8Bytes().ShouldBe("msg2");
        writtenBatches[2][0].MessageContent.ToStringFromUtf8Bytes().ShouldBe("msg3");
    }

    [TestMethod]
    public async Task HighConcurrencyStressTest()
    {
        // Arrange
        var writtenMessages = new List<StoredMessage>();
        var writeLock = new object();
        var storedId = new StoredId(Guid.NewGuid());
        var writeCount = 0;

        var batcher = new MessageBatcher<StoredMessage>(
            async (id, messages) =>
            {
                Interlocked.Increment(ref writeCount);
                await Task.Delay(10); // Simulate write latency
                lock (writeLock)
                {
                    writtenMessages.AddRange(messages);
                }
            }
        );

        const int messageCount = 100;

        // Act - Fire 100 concurrent appends
        var tasks = Enumerable.Range(0, messageCount)
            .Select(i => Task.Run(() => batcher.Handle(storedId, [CreateMessage($"msg{i}")])))
            .ToArray();

        await Task.WhenAll(tasks);

        // Assert
        writtenMessages.Count.ShouldBe(messageCount);
        writeCount.ShouldBeLessThan(messageCount); // Verify batching occurred

        // Verify all messages were written
        var messageContents = writtenMessages
            .Select(m => m.MessageContent.ToStringFromUtf8Bytes())
            .ToHashSet();

        for (int i = 0; i < messageCount; i++)
            messageContents.ShouldContain($"msg{i}");
    }

    [TestMethod]
    public async Task EmptyBatchDoesNotCauseWrite()
    {
        // Arrange
        var writeCount = 0;
        var storedId = new StoredId(Guid.NewGuid());

        var batcher = new MessageBatcher<StoredMessage>(
            (id, messages) =>
            {
                writeCount++;
                return Task.CompletedTask;
            }
        );

        // Act
        await batcher.Handle(storedId, [CreateMessage("msg1")]);

        // Give time for any potential extra writes
        await Task.Delay(100);

        // Assert
        writeCount.ShouldBe(1); // Only one write for the single message
    }
    
    private static StoredMessage CreateMessage(string content) => new(
        Encoding.UTF8.GetBytes(content),
        Encoding.UTF8.GetBytes("System.String"),
        Position: 0
    );
}
