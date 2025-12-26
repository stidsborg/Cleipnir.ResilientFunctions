using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class MessagesSubscriptionTests
{
    public abstract Task EventsSubscriptionSunshineScenario();
    protected async Task EventsSubscriptionSunshineScenario(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestStoredId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId,
            "humanInstanceId",
            Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messageStore = functionStore.MessageStore;

        await messageStore
            .GetMessages(functionId, skip: 0)
            .SelectAsync(msgs => msgs.Any())
            .ShouldBeFalseAsync();

        var events = await messageStore.GetMessages(functionId, skip: 0);
        events.ShouldBeEmpty();

        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world". ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        events = await messageStore.GetMessages(functionId, skip: 0);
        events.Count.ShouldBe(1);
        DefaultSerializer
            .Instance
            .DeserializeMessage(events[0].MessageContent, events[0].MessageType)
            .ShouldBe("hello world");

        var skipPosition = events[0].Position + 1;
        events = await messageStore.GetMessages(functionId, skip: skipPosition);
        events.ShouldBeEmpty();

        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        events = await messageStore.GetMessages(functionId, skip: skipPosition);
        events.Count.ShouldBe(1);

        DefaultSerializer
            .Instance
            .DeserializeMessage(events[0].MessageContent, events[0].MessageType)
            .ShouldBe("hello universe");
    }

    public abstract Task QueueClientCanPullSingleMessage();
    protected async Task QueueClientCanPullSingleMessage(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientCanPullSingleMessage),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId());

                return (string)message;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("test message");

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBe("test message");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task QueueClientCanPullMultipleMessages();
    protected async Task QueueClientCanPullMultipleMessages(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch, watchdogCheckFrequency: TimeSpan.FromMilliseconds(100))
        );

        StoredId? storedId = null;
        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientCanPullMultipleMessages),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                storedId = workflow.StoredId;
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);

                var message1 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId());
                await workflow.Delay(TimeSpan.FromMilliseconds(100));
                var message2 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId());
                await workflow.Delay(TimeSpan.FromMilliseconds(100));
                var message3 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId());
                await workflow.Delay(TimeSpan.FromMilliseconds(100));

                return $"{message1},{message2},{message3}";
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("first");
        await messageWriter.AppendMessage("second");
        await messageWriter.AppendMessage("third");

        var result = await scheduled.Completion(TimeSpan.FromSeconds(5));
        result.ShouldBe("first,second,third");

        var results = await functionStore.EffectsStore.GetEffectResults([storedId!]);
        var x = results.Values.Single();
        Console.WriteLine(x);

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task QueueClientReturnsNullAfterTimeout();
    protected async Task QueueClientReturnsNullAfterTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientReturnsNullAfterTimeout),
            inner: async Task<string?> (string _, Workflow workflow) =>
            {
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), TimeSpan.FromMilliseconds(100));

                return message;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        // No message is sent, so the pull should timeout

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBeNull();

        var cp = await rFunc.ControlPanel("instanceId").ShouldNotBeNullAsync();
        await cp.Messages.Append("hello world");
        var restartResult = await cp.Restart();
        restartResult.ShouldBeNull();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task QueueClientPullsFiveMessagesAndTimesOutOnSixth();
    protected async Task QueueClientPullsFiveMessagesAndTimesOutOnSixth(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var flag = new SyncedFlag();
        
        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientPullsFiveMessagesAndTimesOutOnSixth),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var messages = new List<string>();

                await flag.WaitForRaised();
                
                for (var i = 0; i < 6; i++)
                {
                    var message = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), TimeSpan.FromMilliseconds(250));
                    messages.Add(message ?? "NULL");
                }

                return string.Join(",", messages);
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());

        // Send 5 messages
        await messageWriter.AppendMessage("message1");
        await messageWriter.AppendMessage("message2");
        await messageWriter.AppendMessage("message3");
        await messageWriter.AppendMessage("message4");
        await messageWriter.AppendMessage("message5");

        // Give FetchMessages background task time to fetch the messages
        await Task.Delay(TimeSpan.FromSeconds(1.5));

        flag.Raise();

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBe("message1,message2,message3,message4,message5,NULL");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task OnlyFirstMessageWithSameIdempotencyKeyIsDeliveredAndBothAreRemovedAfterCompletion();
    protected async Task OnlyFirstMessageWithSameIdempotencyKeyIsDeliveredAndBothAreRemovedAfterCompletion(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        StoredId? storedId = null;
        var rFunc = functionsRegistry.RegisterFunc(
            nameof(OnlyFirstMessageWithSameIdempotencyKeyIsDeliveredAndBothAreRemovedAfterCompletion),
            inner: async Task<Tuple<string, string?>> (string _, Workflow workflow) =>
            {
                storedId = workflow.StoredId;
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message1 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId());
                var message2 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), timeout: TimeSpan.FromMilliseconds(100));
                
                return Tuple.Create(message1, message2);
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());

        // Append two messages with the same idempotency key
        await messageWriter.AppendMessage("first message", idempotencyKey: "duplicate-key");
        await messageWriter.AppendMessage("second message", idempotencyKey: "duplicate-key");

        await scheduled.Completion();
        
        await BusyWait.Until(() => storedId != null);
        await BusyWait.Until(async () => await functionStore.MessageStore.GetMessages(storedId!, skip: 0).SelectAsync(m => m.Count) == 0);

        // Only the first message should be delivered
        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.Item1.ShouldBe("first message");
        result.Item2.ShouldBeNull();

        // Verify both messages are removed from the store after completion
        var messagesAfterCompletion = await functionStore.MessageStore.GetMessages(storedId!, skip: 0);
        messagesAfterCompletion.ShouldBeEmpty();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task MultipleIterationsWithDuplicateIdempotencyKeysProcessCorrectly();
    protected async Task MultipleIterationsWithDuplicateIdempotencyKeysProcessCorrectly(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch, watchdogCheckFrequency: TimeSpan.FromMilliseconds(100))
        );

        StoredId? storedId = null;
        var rFunc = functionsRegistry.RegisterFunc(
            nameof(MultipleIterationsWithDuplicateIdempotencyKeysProcessCorrectly),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                storedId = workflow.StoredId;
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var receivedMessages = new List<string>();

                // Pull messages until timeout - expecting 60 unique messages
                var message = "";
                while (message != "stop")
                {
                    message = await queueClient.Pull<string>(
                        workflow,
                        workflow.Effect.CreateNextImplicitId(),
                        TimeSpan.FromMilliseconds(100)
                    );
                    
                    if (message is null)
                        await workflow.Effect.Flush();
                    else if (message is "10" or "20" or "30" or "40")
                    {
                        await workflow.Delay(TimeSpan.FromMilliseconds(100));
                        receivedMessages.Add(message);
                    }
                    else if (message != "stop")
                        receivedMessages.Add(message);
                }

                return string.Join(",", receivedMessages);
            }
        );

        // Schedule the function first
        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        for (var iteration = 0; iteration < 100; iteration += 10)
            for (var repeat = 0; repeat < 2; repeat++)
                for (var i = 0; i < 10; i++)
                await messageWriter.AppendMessage((iteration + i).ToString(), idempotencyKey: ((iteration + i) % 50).ToString());
        
        await BusyWait.Until(() => storedId != null);
        await BusyWait.Until(async () => await functionStore.MessageStore.GetMessages([storedId!]).SelectAsync(m => m[storedId!].Count) == 0, maxWait: TimeSpan.FromSeconds(10));
        await messageWriter.AppendMessage("stop");
        
        // Wait for completion
        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(10));
        var receivedMessages = result
            .Split(',')
            .Select(int.Parse)
            .OrderBy(_ => _)
            .ToList();

        receivedMessages.Count.ShouldBe(50);
        for (var i = 0; i < 50; i++)
            receivedMessages[i].ShouldBe(i);
        
        await BusyWait.Until(
            async () => await functionStore.MessageStore.GetMessages(storedId!, skip: 0).SelectAsync(m => m.Count) == 0
        );

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
}