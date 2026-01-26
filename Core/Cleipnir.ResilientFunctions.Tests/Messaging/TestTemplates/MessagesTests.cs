using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class MessagesTests
{
    public abstract Task MessagesSunshineScenario();
    protected async Task MessagesSunshineScenario(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        QueueClient? queueClient = null;
        var rFunc = functionsRegistry.RegisterFunc(
            nameof(MessagesSunshineScenario),
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
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), maxWait: TimeSpan.FromMinutes(1));

                return message;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");

        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("hello world");
        await BusyWait.Until(() => queueClient is not null);
        await queueClient!.FetchMessages(); // Immediately fetch the message

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBe("hello world");

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
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), TimeSpan.FromMilliseconds(100), maxWait: TimeSpan.FromMinutes(1));

                return message;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBeNull();

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout();
    protected async Task MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout),
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
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message = await queueClient.Pull<object>(
                    workflow,
                    workflow.Effect.CreateNextImplicitId(),
                    TimeSpan.Zero,
                    filter: m => m is string or int,
                    maxWait: TimeSpan.FromMinutes(1)
                );

                return message == null ? "NONE" : message.ToString()!;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBe("NONE");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst();
    protected async Task MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst),
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
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message = await queueClient.Pull<object>(
                    workflow,
                    workflow.Effect.CreateNextImplicitId(),
                    filter: m => m is string or int,
                    maxWait: TimeSpan.FromMinutes(1)
                );

                return message!.ToString()!;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("Hello");

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBe("Hello");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond();
    protected async Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var functionsRegistry = new FunctionsRegistry(
            functionStore,
            new Settings(unhandledExceptionCatcher.Catch)
        );

        var rFunc = functionsRegistry.RegisterFunc(
            nameof(MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond),
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
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message = await queueClient.Pull<string>(
                    workflow,
                    workflow.Effect.CreateNextImplicitId(),
                    maxWait: TimeSpan.FromMinutes(1)
                );

                return message;
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());
        await messageWriter.AppendMessage("1");

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.ShouldBe("1");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnored();
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnored(Task<IFunctionStore> functionStoreTask)
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
            nameof(SecondEventWithExistingIdempotencyKeyIsIgnored),
            inner: async Task<Tuple<string, string>> (string _, Workflow workflow) =>
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
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message1 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), maxWait: TimeSpan.FromMinutes(1));
                var message2 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), maxWait: TimeSpan.FromMinutes(1));

                return Tuple.Create(message1, message2);
            }
        );

        var scheduled = await rFunc.Schedule("instanceId", "");
        var messageWriter = rFunc.MessageWriters.For("instanceId".ToFlowInstance());

        await messageWriter.AppendMessage("hello world", idempotencyKey: "1");
        await messageWriter.AppendMessage("hello world", idempotencyKey: "1");
        await messageWriter.AppendMessage("hello universe");

        var result = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(5));
        result.Item1.ShouldBe("hello world");
        result.Item2.ShouldBe("hello universe");

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
            new Settings(unhandledExceptionCatcher.Catch)
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
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);

                var message1 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), maxWait: TimeSpan.FromMinutes(1));
                var message2 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), maxWait: TimeSpan.FromMinutes(1));

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
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var registry = new FunctionsRegistry(functionStore, new Settings(unhandledExceptionCatcher.Catch));

        var queueClients = new Dictionary<string, QueueClient>();
        var registration = registry.RegisterParamless(
            flowType,
            async Task (workflow) =>
            {
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                lock (queueClients)
                    queueClients[workflow.FlowId.Instance.Value] = queueClient;

                await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), maxWait: TimeSpan.FromMinutes(1));
            }
        );

        await registration.Schedule("Instance#1");
        await registration.Schedule("Instance#2");

        await BusyWait.Until(() =>
        {
            lock (queueClients)
                return queueClients.ContainsKey("Instance#1") && queueClients.ContainsKey("Instance#2");
        });

        await registration.SendMessages(
            [
                new BatchedMessage("Instance#1", "hallo world", IdempotencyKey: "1"),
                new BatchedMessage("Instance#2", "hallo world", IdempotencyKey: "1")
            ]
        );

        // Immediately fetch messages for both workflows
        await queueClients["Instance#1"].FetchMessages();
        await queueClients["Instance#2"].FetchMessages();

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
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var registry = new FunctionsRegistry(functionStore, new Settings(unhandledExceptionCatcher.Catch));
        var messages = new List<string>();
        var registration = registry.RegisterParamless(
            flowType,
            async Task (workflow) =>
            {
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);

                while (true)
                {
                    var message = await queueClient.Pull<object>(workflow, workflow.Effect.CreateNextImplicitId(), maxWait: TimeSpan.FromSeconds(10));
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
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
        using var registry = new FunctionsRegistry(functionStore, new Settings(unhandledExceptionCatcher.Catch, messagesPullFrequency: TimeSpan.FromMilliseconds(10)));
        ParamlessRegistration pongRegistration = null!;
        ParamlessRegistration pingRegistration = null!;

        pingRegistration = registry.RegisterParamless(
            "PingFlow",
            async Task (workflow) =>
            {
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);

                for (var i = 0; i < 10; i++)
                {
                    await pongRegistration.SendMessage("Pong", new Ping(i), idempotencyKey: $"Pong{i}");
                    await queueClient.Pull<Pong>(workflow, workflow.Effect.CreateNextImplicitId(), filter: pong => pong.Number == i, maxWait: TimeSpan.FromMinutes(1));
                }
            });

        pongRegistration = registry.RegisterParamless(
            "PongFlow",
            async Task (workflow) =>
            {
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);

                for (var i = 0; i < 10; i++)
                {
                    await queueClient.Pull<Ping>(workflow, workflow.Effect.CreateNextImplicitId(), filter: ping => ping.Number == i, maxWait: TimeSpan.FromMinutes(1));
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
    
    public abstract Task NoOpMessageIsIgnored();
    protected async Task NoOpMessageIsIgnored(Task<IFunctionStore> functionStoreTask)
    {
        var flowType = TestFlowId.Create().Type;
        var functionStore = await functionStoreTask;
        functionStore = functionStore.WithPrefix("NoOp" + Guid.NewGuid().ToString("N"));
        await functionStore.Initialize();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);

        using var registry = new FunctionsRegistry(functionStore, new Settings(unhandledExceptionCatcher.Catch));
        var registration = registry.RegisterFunc<string, string>(
            flowType,
            async Task<string> (_, workflow) =>
            {
                var queueManager = new QueueManager(
                    workflow.FlowId,
                    workflow.StoredId,
                    functionStore.MessageStore,
                    DefaultSerializer.Instance,
                    workflow.Effect,
                    unhandledExceptionHandler,
                    new FlowMinimumTimeout(),
                    () => DateTime.UtcNow,
                    SettingsWithDefaults.Default
                );
                await queueManager.Initialize();

                var queueClient = new QueueClient(queueManager, () => DateTime.UtcNow);
                var message = await queueClient.Pull<object>(workflow, workflow.Effect.CreateNextImplicitId(), maxWait: TimeSpan.FromSeconds(10));

                message.ShouldBeOfType<string>();
                return (string)message;
            }
        );

        var invocation = registration.Invoke("SomeInstance", "SomeParam");

        await registration.SendMessage("SomeInstance", NoOp.Instance);
        await registration.SendMessage("SomeInstance", "Hallo World!");

        var result = await invocation;
        result.ShouldBe("Hallo World!");

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

}