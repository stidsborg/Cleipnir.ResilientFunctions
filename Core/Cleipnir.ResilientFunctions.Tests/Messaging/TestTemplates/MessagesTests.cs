using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class MessagesTests
{
    private static Settings CreateSettings(
        Action<FrameworkException> unhandledExceptionHandler,
        TimeSpan? messagesPullFrequency = null) =>
        new(
            unhandledExceptionHandler,
            watchdogCheckFrequency: TimeSpan.FromMilliseconds(100),
            messagesPullFrequency: messagesPullFrequency ?? TimeSpan.FromMilliseconds(100)
        );

    public abstract Task MessagesSunshineScenario();
    protected async Task MessagesSunshineScenario(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            CreateSettings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(MessagesSunshineScenario),
            inner: async Task<string> (string _, Workflow workflow)
                => await workflow.Message<string>()
        );

        var scheduled = await rFunc.Schedule("instanceId", "");

        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("hello world");

        var result = await scheduled.Completion(timeout: TimeSpan.FromSeconds(5));
        result.ShouldBe("hello world");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task QueueClientReturnsNullAfterTimeout();
    protected async Task QueueClientReturnsNullAfterTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            CreateSettings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientReturnsNullAfterTimeout),
            inner: async Task<string?> (string _, Workflow workflow)
                => await workflow.Message<string>(TimeSpan.FromMilliseconds(100))
        );

        var scheduled = await rFunc.Schedule("instanceId", "");

        var result = await scheduled.Completion(timeout: TimeSpan.FromSeconds(5));
        result.ShouldBeNull();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout();
    protected async Task MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            CreateSettings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<object>(
                    filter: m => m is string or int,
                    waitFor: TimeSpan.Zero
                );
                return message == null ? "NONE" : message.ToString()!;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");

        var result = await scheduled.Completion(timeout: TimeSpan.FromSeconds(5));
        result.ShouldBe("NONE");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst();
    protected async Task MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            CreateSettings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var message = await workflow.Message<object>(filter: m => m is string or int);
                return message.ToString()!;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("Hello");

        var result = await scheduled.Completion(timeout: TimeSpan.FromSeconds(5));
        result.ShouldBe("Hello");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond();
    protected async Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            CreateSettings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond),
            inner: async Task<string> (string _, Workflow workflow)
                => await workflow.Message<string>()
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("1");

        var result = await scheduled.Completion(timeout: TimeSpan.FromSeconds(5));
        result.ShouldBe("1");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnored();
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnored(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            CreateSettings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(SecondEventWithExistingIdempotencyKeyIsIgnored),
            inner: async Task<Tuple<string, string>> (string _, Workflow workflow) =>
            {
                var message1 = await workflow.Message<string>();
                var message2 = await workflow.Message<string>();
                return Tuple.Create(message1, message2);
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());

        await messageWriter.AppendMessage("hello world", idempotencyKey: "1");
        await messageWriter.AppendMessage("hello world", idempotencyKey: "1");
        await messageWriter.AppendMessage("hello universe");

        var result = await scheduled.Completion(timeout: TimeSpan.FromSeconds(5));
        result.Item1.ShouldBe("hello world");
        result.Item2.ShouldBe("hello universe");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task QueueClientCanPullMultipleMessages();
    protected async Task QueueClientCanPullMultipleMessages(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            CreateSettings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(QueueClientCanPullMultipleMessages),
            inner: async Task<string> (string _, Workflow workflow) =>
            {
                var message1 = await workflow.Message<string>();
                var message2 = await workflow.Message<string>();
                return $"{message1},{message2}";
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());

        await messageWriter.AppendMessage("hello world", "1");
        await messageWriter.AppendMessage("hello world", "1");
        await messageWriter.AppendMessage("hello universe");

        var result = await scheduled.Completion(TimeSpan.FromSeconds(5));
        result.ShouldBe("hello world,hello universe");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }


    public abstract Task BatchedMessagesIsDeliveredToAwaitingFlows();
    protected async Task BatchedMessagesIsDeliveredToAwaitingFlows(Task<IFunctionStore> functionStoreTask)
    {
        var flowType = TestFlowId.Create().Type;
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var registry = new FunctionsRegistry(functionStore, CreateSettings(unhandledExceptionCatcher.Catch));

        var registration = registry.RegisterParamless(
            flowType,
            async Task (workflow) =>
            {
                await workflow.Message<string>();
            }
        );

        await registration.Schedule("Instance#1");
        await registration.Schedule("Instance#2");

        await registration.SendMessages(
            [
                new BatchedMessage("Instance#1", "hallo world", IdempotencyKey: "1"),
                new BatchedMessage("Instance#2", "hallo world", IdempotencyKey: "1")
            ]
        );

        var controlPanel1 = await registration.ControlPanel("Instance#1").ShouldNotBeNullAsync();
        var controlPanel2 = await registration.ControlPanel("Instance#2").ShouldNotBeNullAsync();

        await controlPanel1.WaitForCompletion(maxWait: TimeSpan.FromSeconds(2));
        await controlPanel2.WaitForCompletion(maxWait: TimeSpan.FromSeconds(2));

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    public abstract Task MultipleMessagesCanBeAppendedOneAfterTheOther();
    protected async Task MultipleMessagesCanBeAppendedOneAfterTheOther(Task<IFunctionStore> functionStoreTask)
    {
        var flowType = TestFlowId.Create().Type;
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var registry = new FunctionsRegistry(functionStore, CreateSettings(unhandledExceptionCatcher.Catch));
        var messages = new List<string>();
        var registration = registry.RegisterParamless(
            flowType,
            async Task (workflow) =>
            {
                while (true)
                {
                    var message = await workflow.Message<object>();
                    if (message is string s)
                        await workflow.Effect.Capture(() => messages.Add(s));
                    else
                        return;
                }
            });

        var instanceId = "Instance#1";

        await registration.SendMessage(instanceId, "Hallo");
        await registration.SendMessage(instanceId, "World");
        await registration.SendMessage(instanceId, "And");
        await registration.SendMessage(instanceId, "Universe");
        await registration.SendMessage(instanceId, new object());

        var cp = await registration.ControlPanel(instanceId).ShouldNotBeNullAsync();
        await cp.WaitForCompletion(allowPostponeAndSuspended: true);

        messages.Count.ShouldBe(4);
        messages[0].ShouldBe("Hallo");
        messages[1].ShouldBe("World");
        messages[2].ShouldBe("And");
        messages[3].ShouldBe("Universe");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    private record Ping(int Number);
    private record Pong(int Number);

    public abstract Task PingPongMessagesCanBeExchangedMultipleTimes();
    protected async Task PingPongMessagesCanBeExchangedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        functionStore = functionStore.WithPrefix("pingpong" + Guid.NewGuid().ToString("N"));
        await functionStore.Initialize();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var registry = new FunctionsRegistry(functionStore, CreateSettings(unhandledExceptionCatcher.Catch, messagesPullFrequency: TimeSpan.FromMilliseconds(10)));
        ParamlessRegistration pongRegistration = null!;
        ParamlessRegistration pingRegistration = null!;

        pingRegistration = registry.RegisterParamless(
            "PingFlow",
            async Task (workflow) =>
            {
                for (var i = 0; i < 10; i++)
                {
                    await pongRegistration.SendMessage("Pong", new Ping(i), idempotencyKey: $"Pong{i}");
                    await workflow.Message<Pong>(filter: pong => pong.Number == i);
                }
            });

        pongRegistration = registry.RegisterParamless(
            "PongFlow",
            async Task (workflow) =>
            {
                for (var i = 0; i < 10; i++)
                {
                    await workflow.Message<Ping>(filter: ping => ping.Number == i);
                    await pingRegistration.SendMessage("Ping", new Pong(i), idempotencyKey: $"Ping{i}");
                }
            });

        await pongRegistration.Schedule("Pong");
        await pingRegistration.Schedule("Ping");

        var pongCp = await pongRegistration.ControlPanel("Pong").ShouldNotBeNullAsync();
        var pingCp = await pingRegistration.ControlPanel("Ping").ShouldNotBeNullAsync();

        await pongCp.WaitForCompletion(allowPostponeAndSuspended: true);
        await pingCp.WaitForCompletion(allowPostponeAndSuspended: true);

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task SenderIsPersistedAndCanBeFetched();
    protected async Task SenderIsPersistedAndCanBeFetched(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var storedType = new StoredType(1);
        var storedId = StoredId.Create(storedType, "instanceId");
        var serializer = DefaultSerializer.Instance;
        var messageStore = functionStore.MessageStore;

        var messageWriter = new MessageWriter(storedId, messageStore, serializer);
        await messageWriter.AppendMessage("hello world", idempotencyKey: "key1", sender: "TestSender");

        var messages = await messageStore.GetMessages(storedId);
        messages.Count.ShouldBe(1);
        messages[0].Sender.ShouldBe("TestSender");
    }
}