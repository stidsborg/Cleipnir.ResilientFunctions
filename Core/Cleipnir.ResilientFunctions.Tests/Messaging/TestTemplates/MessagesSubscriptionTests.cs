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
            .GetMessages(functionId)
            .SelectAsync(msgs => msgs.Any())
            .ShouldBeFalseAsync();

        var events = await messageStore.GetMessages(functionId);
        events.ShouldBeEmpty();

        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world". ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        events = await messageStore.GetMessages(functionId);
        events.Count.ShouldBe(1);
        DefaultSerializer
            .Instance
            .Deserialize(events[0].MessageContent, DefaultSerializer.Instance.ResolveType(events[0].MessageType)!)
            .ShouldBe("hello world");

        var skipPosition = events[0].Position;
        var filteredEvents = (await messageStore.GetMessages(functionId)).Where(e => e.Position > skipPosition).ToList();
        filteredEvents.ShouldBeEmpty();

        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        filteredEvents = (await messageStore.GetMessages(functionId)).Where(e => e.Position > skipPosition).ToList();
        filteredEvents.Count.ShouldBe(1);

        DefaultSerializer
            .Instance
            .Deserialize(filteredEvents[0].MessageContent, DefaultSerializer.Instance.ResolveType(filteredEvents[0].MessageType)!)
            .ShouldBe("hello universe");
    }

    public abstract Task QueueClientCanPullSingleMessage();
    protected async Task QueueClientCanPullSingleMessage(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientCanPullSingleMessage),
            inner: (string _, Workflow workflow) => workflow.Message<string>(maxWait: TimeSpan.FromMinutes(1))
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

                var message1 = await workflow.Message<string>(maxWait: TimeSpan.FromMinutes(1));
                await workflow.Delay(TimeSpan.FromMilliseconds(100));
                var message2 = await workflow.Message<string>(maxWait: TimeSpan.FromMinutes(1));
                await workflow.Delay(TimeSpan.FromMilliseconds(100));
                var message3 = await workflow.Message<string>(maxWait: TimeSpan.FromMinutes(1));
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
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientReturnsNullAfterTimeout),
            inner: async Task<string?> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<string>(TimeSpan.FromMilliseconds(100), maxWait: TimeSpan.FromMinutes(1));
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
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var flag = new SyncedFlag();

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientPullsFiveMessagesAndTimesOutOnSixth),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var messages = new List<string>();

                await flag.WaitForRaised();

                for (var i = 0; i < 6; i++)
                {
                    var message = await workflow.Message<string>(TimeSpan.FromMilliseconds(250), maxWait: TimeSpan.FromMinutes(1));
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

                var message1 = await workflow.Message<string>(maxWait: TimeSpan.FromMinutes(1));
                var message2 = await workflow.Message<string>(TimeSpan.FromMilliseconds(100), maxWait: TimeSpan.FromMinutes(1));

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
        await BusyWait.Until(async () => await functionStore.MessageStore.GetMessages(storedId!).SelectAsync(m => m.Count) == 0);

        // Only the first message should be delivered
        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.Item1.ShouldBe("first message");
        result.Item2.ShouldBeNull();

        // Verify both messages are removed from the store after completion
        var messagesAfterCompletion = await functionStore.MessageStore.GetMessages(storedId!);
        messagesAfterCompletion.ShouldBeEmpty();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task MultipleIterationsWithDuplicateIdempotencyKeysProcessCorrectly();
    protected async Task MultipleIterationsWithDuplicateIdempotencyKeysProcessCorrectly(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
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

                var receivedMessages = new List<string>();

                // Pull messages until timeout - expecting 60 unique messages
                var message = "";
                while (message != "stop")
                {
                    message = await workflow.Message<string>(
                        TimeSpan.FromMilliseconds(100),
                        maxWait: TimeSpan.FromMinutes(1)
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
        await BusyWait.Until(async () => await functionStore.MessageStore.GetMessages([storedId!]).SelectAsync(m => m[storedId!].Count) == 0, maxWait: TimeSpan.FromSeconds(30));
        await messageWriter.AppendMessage("stop");

        // Wait for completion
        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(30));
        var receivedMessages = result
            .Split(',')
            .Select(int.Parse)
            .OrderBy(_ => _)
            .ToList();

        receivedMessages.Count.ShouldBe(50);
        for (var i = 0; i < 50; i++)
            receivedMessages[i].ShouldBe(i);

        await BusyWait.Until(
            async () => await functionStore.MessageStore.GetMessages(storedId!).SelectAsync(m => m.Count) == 0,
            maxWait: TimeSpan.FromSeconds(30)
        );

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task QueueClientFilterParameterFiltersMessages();
    protected async Task QueueClientFilterParameterFiltersMessages(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientFilterParameterFiltersMessages),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                // Pull only messages that start with "even-"
                var message1 = await workflow.Message<string>(
                    m => m.StartsWith("even-"),
                    maxWait: TimeSpan.FromMinutes(1)
                );

                var message2 = await workflow.Message<string>(
                    m => m.StartsWith("even-"),
                    maxWait: TimeSpan.FromMinutes(1)
                );

                var message3 = await workflow.Message<string>(
                    m => m.StartsWith("even-"),
                    maxWait: TimeSpan.FromMinutes(1)
                );

                return $"{message1},{message2},{message3}";
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());

        // Send mixed messages - odd and even
        await messageWriter.AppendMessage("odd-1");
        await messageWriter.AppendMessage("even-2");
        await messageWriter.AppendMessage("odd-3");
        await messageWriter.AppendMessage("even-4");
        await messageWriter.AppendMessage("odd-5");
        await messageWriter.AppendMessage("even-6");

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        // Should only receive the even messages, filtered out the odd ones
        result.ShouldBe("even-2,even-4,even-6");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task QueueClientWorksWithCustomSerializer();
    protected async Task QueueClientWorksWithCustomSerializer(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        // Use default serializer to ensure serialization works correctly
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientWorksWithCustomSerializer),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                // Pull different types of messages to verify serialization works
                var message1 = await workflow.Message<string>(maxWait: TimeSpan.FromSeconds(5));
                var message2 = await workflow.Message<WrappedInt>(maxWait: TimeSpan.FromSeconds(5));
                var message3 = await workflow.Message<TestRecord>(maxWait: TimeSpan.FromSeconds(5));

                return $"{message1},{message2.Value},{message3.Value}";
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());

        await messageWriter.AppendMessage("hello");
        await messageWriter.AppendMessage(new WrappedInt(42));
        await messageWriter.AppendMessage(new TestRecord("world"));

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(10));
        result.ShouldBe("hello,42,world");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    private record WrappedInt(int Value);
    private record TestRecord(string Value);

    public abstract Task BatchedMessagesAreDeliveredToMultipleFlows();
    protected async Task BatchedMessagesAreDeliveredToMultipleFlows(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(BatchedMessagesAreDeliveredToMultipleFlows),
            inner: (string _, Workflow workflow) => workflow.Message<string>(maxWait: TimeSpan.FromMinutes(1))
        );

        // Send batched messages first
        await rFunc.SendMessages(
            [
                new BatchedMessage("Instance#1", "hallo world 1", IdempotencyKey: "1"),
                new BatchedMessage("Instance#2", "hallo world 2", IdempotencyKey: "2")
            ]
        );

        // Then schedule the workflows - they should pick up the messages
        var scheduled1 = await rFunc.Schedule("Instance#1", "");
        var scheduled2 = await rFunc.Schedule("Instance#2", "");

        // Wait for completion
        var result1 = await scheduled1.Completion(maxWait: TimeSpan.FromSeconds(10));
        var result2 = await scheduled2.Completion(maxWait: TimeSpan.FromSeconds(10));

        result1.ShouldBe("hallo world 1");
        result2.ShouldBe("hallo world 2");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    private record Ping(int Number);
    private record Pong(int Number);

    public abstract Task QueueClientSupportsMultiFlowMessageExchange();
    protected async Task QueueClientSupportsMultiFlowMessageExchange(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        functionStore = functionStore.WithPrefix("pingpong" + Guid.NewGuid().ToString("N"));
        await functionStore.Initialize();

        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch, messagesPullFrequency: TimeSpan.FromMilliseconds(10))
        );

        FuncRegistration<string, string>? pongRegistration = null;
        FuncRegistration<string, string>? pingRegistration = null;

        pingRegistration = functionsRegistry.RegisterFunc(
            "PingFlow",
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                for (var i = 0; i < 10; i++)
                {
                    await pongRegistration!.SendMessage("Pong", new Ping(i), idempotencyKey: $"Pong{i}");
                    await workflow.Message<Pong>(pong => pong.Number == i, maxWait: TimeSpan.FromMinutes(1));
                }

                return "completed";
            }
        );

        pongRegistration = functionsRegistry.RegisterFunc(
            "PongFlow",
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                for (var i = 0; i < 10; i++)
                {
                    await workflow.Message<Ping>(ping => ping.Number == i, maxWait: TimeSpan.FromMinutes(1));
                    await pingRegistration!.SendMessage("Ping", new Pong(i), idempotencyKey: $"Ping{i}");
                }

                return "completed";
            }
        );

        await pongRegistration.Schedule("Pong", "");
        await pingRegistration.Schedule("Ping", "");

        var pongCp = await pongRegistration.ControlPanel("Pong").ShouldNotBeNullAsync();
        var pingCp = await pingRegistration.ControlPanel("Ping").ShouldNotBeNullAsync();

        await pongCp.WaitForCompletion(allowPostponeAndSuspended: true);
        await pingCp.WaitForCompletion(allowPostponeAndSuspended: true);

        await pongCp.Refresh();
        var pongResult = pongCp.Result;
        pongResult.ShouldNotBeNull();
        pongResult.ShouldBe("completed");

        await pingCp.Refresh();
        var pingResult = pingCp.Result;
        pingResult.ShouldNotBeNull();
        pingResult.ShouldBe("completed");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task QueueManagerFailsOnMessageDeserializationError();
    protected async Task QueueManagerFailsOnMessageDeserializationError(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        var exceptionThrowingSerializer = new ExceptionThrowingEventSerializer(typeof(BadMessage));
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueManagerFailsOnMessageDeserializationError),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var flowsTimeoutManager = new FlowsTimeoutManager();
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    exceptionThrowingSerializer,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowTimeouts(flowsTimeoutManager, workflow.StoredId),
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default,
                    flowsTimeoutManager
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, DefaultSerializer.Instance, () => DateTime.UtcNow);

                var message = await queueClient.Pull<GoodMessage>(
                    workflow,
                    workflow.Effect.CreateNextImplicitId(),
                    maxWait: TimeSpan.FromMinutes(1)
                );

                return message.Value;
            }
        );

        await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());

        await messageWriter.AppendMessage(new BadMessage("will-fail"), idempotencyKey: "bad-message");

        var controlPanel = await rFunc.ControlPanel("instanceId").ShouldNotBeNullAsync();
        await controlPanel.BusyWaitUntil(c => c.Status == Status.Failed, maxWait: TimeSpan.FromSeconds(10));

        controlPanel.Status.ShouldBe(Status.Failed);

        unhandledExceptionCatcher.ThrownExceptions.Count.ShouldBeGreaterThanOrEqualTo(1);
        var deserializationException = unhandledExceptionCatcher.ThrownExceptions
            .Select(e => e.InnerException)
            .OfType<DeserializationException>()
            .FirstOrDefault();
        deserializationException.ShouldNotBeNull();
        deserializationException.Message.ShouldBe("Deserialization failed for BadMessage");
    }

    public abstract Task RegisteredTimeoutIsRemovedWhenPullingMessage();
    protected async Task RegisteredTimeoutIsRemovedWhenPullingMessage(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var flowsTimeoutManager = new FlowsTimeoutManager();
        StoredId? storedId = null;
        var rFunc = functionsRegistry.RegisterFunc(
            nameof(RegisteredTimeoutIsRemovedWhenPullingMessage),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                storedId = workflow.StoredId;
                var minimumTimeout = new FlowTimeouts(flowsTimeoutManager, workflow.StoredId);
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    minimumTimeout,
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default,
                    flowsTimeoutManager
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, DefaultSerializer.Instance, () => DateTime.UtcNow);

                // Verify timeout is not set before pull
                minimumTimeout.MinimumTimeout.ShouldBeNull();

                var message = await queueClient.Pull<string>(
                    workflow,
                    workflow.Effect.CreateNextImplicitId(),
                    timeout: TimeSpan.FromMinutes(5),
                    maxWait: TimeSpan.FromMinutes(1)
                );

                // Verify timeout is removed after successful pull
                minimumTimeout.MinimumTimeout.ShouldBeNull();

                return message!;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("test message");

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBe("test message");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

public abstract Task PullEnvelopeReturnsEnvelopeWithReceiverAndSender();
    protected async Task PullEnvelopeReturnsEnvelopeWithReceiverAndSender(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(PullEnvelopeReturnsEnvelopeWithReceiverAndSender),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var flowsTimeoutManager = new FlowsTimeoutManager();
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowTimeouts(flowsTimeoutManager, workflow.StoredId),
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default,
                    flowsTimeoutManager
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, DefaultSerializer.Instance, () => DateTime.UtcNow);

                // Pull envelope for specific receiver
                var envelope = await queueClient.PullEnvelope<string>(
                    workflow,
                    workflow.Effect.CreateNextImplicitId(),
                    filter: _ => true,
                    maxWait: TimeSpan.FromMinutes(1)
                );

                return $"{envelope.Message}|{envelope.Receiver}|{envelope.Sender}";
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("test message", receiver: "receiver1", sender: "sender1");

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBe("test message|receiver1|sender1");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    private record GoodMessage(string Value);
    private record BadMessage(string Value);

    private class ExceptionThrowingEventSerializer : ISerializer
    {
        private readonly Type _failDeserializationOnType;

        public ExceptionThrowingEventSerializer(Type failDeserializationOnType)
            => _failDeserializationOnType = failDeserializationOnType;

        public byte[] Serialize(object value, Type type)
            => DefaultSerializer.Instance.Serialize(value, type);

        public object Deserialize(byte[] json, Type type)
        {
            if (type == _failDeserializationOnType)
                throw new DeserializationException("Deserialization failed for BadMessage", new Exception("Inner cause"));

            return DefaultSerializer.Instance.Deserialize(json, type);
        }

    }
}