using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.Messaging.TestTemplates;

public abstract class MessagesTests
{
    public abstract Task MessagesSunshineScenario();
    protected async Task MessagesSunshineScenario(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var minimumTimeout = new FlowMinimumTimeout();
        using var registeredTimeouts = new FlowRegisteredTimeouts(
            CreateEffect(storedId, flowId, functionStore, minimumTimeout), 
            () => DateTime.UtcNow, 
            minimumTimeout, 
            t => messagesWriter.AppendMessage(t),
            new UnhandledExceptionHandler(_ => {}),
            flowId
        );
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue, 
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            registeredTimeouts,
            initialMessages: [],
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);
        
        var task = messages.First();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await messages.AppendMessage("hello world");

        (await task).ShouldBe("hello world");
    }
    
    public abstract Task MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout();
    protected async Task MessagesFirstOfTypesReturnsNoneForFirstOfTypesOnTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;

        var functionRegistry = new FunctionsRegistry(
            functionStore,
            settings: new Settings(watchdogCheckFrequency: TimeSpan.FromSeconds(1), messagesDefaultMaxWaitForCompletion: TimeSpan.MaxValue)
            );

        var registration = functionRegistry.RegisterParamless(
            flowId.Type,
            async workflow =>
            {
                var messages = workflow.Messages;
                var eitherOrNone = await messages.FirstOfTypes<string, int>(expiresIn: TimeSpan.Zero);
                eitherOrNone.HasNone.ShouldBeTrue();
                eitherOrNone.HasFirst.ShouldBeFalse();
                eitherOrNone.HasSecond.ShouldBeFalse();
            });


        await registration.Invoke(flowId.Instance);
    }
    
    public abstract Task MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst();
    protected async Task MessagesFirstOfTypesReturnsFirstForFirstOfTypesOnFirst(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var minimumTimeout = new FlowMinimumTimeout();
        using var registeredTimeouts = new FlowRegisteredTimeouts(
            CreateEffect(storedId, flowId, functionStore, minimumTimeout), 
            () => DateTime.UtcNow, 
            minimumTimeout, 
            t => messagesWriter.AppendMessage(t),
            new UnhandledExceptionHandler(_ => {}),
            flowId
        );        
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue, 
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            registeredTimeouts,
            initialMessages: [],
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);
        
        var eitherOrNoneTask = messages.FirstOfTypes<string, int>(expiresIn: TimeSpan.FromSeconds(10));

        await messages.AppendMessage("Hello");

        var eitherOrNone = await eitherOrNoneTask;
        eitherOrNone.HasFirst.ShouldBeTrue();
        eitherOrNone.First.ShouldBe("Hello");
        eitherOrNone.HasNone.ShouldBeFalse();
        eitherOrNone.HasSecond.ShouldBeFalse();
    }
    
    public abstract Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond();
    protected async Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var minimumTimeout = new FlowMinimumTimeout();
        using var registeredTimeouts = new FlowRegisteredTimeouts(
            CreateEffect(storedId, flowId, functionStore, minimumTimeout), 
            () => DateTime.UtcNow, 
            minimumTimeout, 
            t => messagesWriter.AppendMessage(t),
            new UnhandledExceptionHandler(_ => {}),
            flowId
        );
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue, 
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            registeredTimeouts,
            initialMessages: [],
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);
        
        var eitherOrNoneTask = messages.FirstOfTypes<string, int>(expiresIn: TimeSpan.FromSeconds(10));

        await messages.AppendMessage(1);

        var eitherOrNone = await eitherOrNoneTask;
        eitherOrNone.HasSecond.ShouldBeTrue();
        eitherOrNone.Second.ShouldBe(1);
        eitherOrNone.HasNone.ShouldBeFalse();
        eitherOrNone.HasFirst.ShouldBeFalse();
    }
    
    public abstract Task ExistingEventsShouldBeSameAsAllAfterEmit();
    protected async Task ExistingEventsShouldBeSameAsAllAfterEmit(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var minimumTimeout = new FlowMinimumTimeout();
        using var registeredTimeouts = new FlowRegisteredTimeouts(
            CreateEffect(storedId, flowId, functionStore, minimumTimeout), 
            () => DateTime.UtcNow, 
            minimumTimeout, 
            t => messagesWriter.AppendMessage(t),
            new UnhandledExceptionHandler(_ => {}),
            flowId
        );
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            registeredTimeouts,
            initialMessages: [],
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);

        await messages.AppendMessage("hello world");

        var nextEvent = await messages.First();
        nextEvent.ShouldBe("hello world");

        var next = messages
            .OfType<string>()
            .Existing(out _)
            .First();
        
        next.ShouldBe("hello world");
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnored();
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnored(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var minimumTimeout = new FlowMinimumTimeout();
        using var registeredTimeouts = new FlowRegisteredTimeouts(
            CreateEffect(storedId, flowId, functionStore, minimumTimeout), 
            () => DateTime.UtcNow, 
            minimumTimeout, 
            t => messagesWriter.AppendMessage(t),
            new UnhandledExceptionHandler(_ => {}),
            flowId
        );        
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            registeredTimeouts,
            initialMessages: [],
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);
        
        var task = messages.Take(2).ToList();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await messages.AppendMessage("hello world", idempotencyKey: "1");
        await messages.AppendMessage("hello world", idempotencyKey: "1");
        await messages.AppendMessage("hello universe");

        await BusyWait.Until(() => task.IsCompleted);
        task.IsCompletedSuccessfully.ShouldBeTrue();
        task.Result.Count.ShouldBe(2);
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await functionStore.MessageStore.GetMessages(storedId, skip: 0)).Count().ShouldBe(3);
    }
    
    public abstract Task MessagesBulkMethodOverloadAppendsAllEventsSuccessfully();
    protected async Task MessagesBulkMethodOverloadAppendsAllEventsSuccessfully(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var minimumTimeout = new FlowMinimumTimeout();
        using var registeredTimeouts = new FlowRegisteredTimeouts(
            CreateEffect(storedId, flowId, functionStore, minimumTimeout), 
            () => DateTime.UtcNow, 
            minimumTimeout, 
            t => messagesWriter.AppendMessage(t),
            new UnhandledExceptionHandler(_ => {}),
            flowId
        );        
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            registeredTimeouts,
            initialMessages: [],
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);

        var task = messages.Take(2).ToList();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();
        await messages.AppendMessage("hello world", "1");
        await messages.AppendMessage("hello world", "1");
        await messages.AppendMessage("hello universe");

        await BusyWait.Until(() => task.IsCompletedSuccessfully);
        
        task.Result.Count.ShouldBe(2);
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await functionStore.MessageStore.GetMessages(storedId, skip: 0)).Count().ShouldBe(3);
    }

    public abstract Task MessagessSunshineScenarioUsingMessageStore();
    protected async Task MessagessSunshineScenarioUsingMessageStore(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var minimumTimeout = new FlowMinimumTimeout();
        using var registeredTimeouts = new FlowRegisteredTimeouts(
            CreateEffect(storedId, flowId, functionStore, minimumTimeout), 
            () => DateTime.UtcNow, 
            minimumTimeout, 
            t => messagesWriter.AppendMessage(t),
            new UnhandledExceptionHandler(_ => {}),
            flowId
        );        
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            registeredTimeouts,
            initialMessages: [],
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);
        
        var task = messages.First();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await functionStore.MessageStore.AppendMessage(
            storedId,
            new StoredMessage(JsonExtensions.ToJson("hello world").ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );

        (await task).ShouldBe("hello world");
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore();
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var minimumTimeout = new FlowMinimumTimeout();
        using var registeredTimeouts = new FlowRegisteredTimeouts(
            CreateEffect(storedId, flowId, functionStore, minimumTimeout), 
            () => DateTime.UtcNow, 
            minimumTimeout, 
            t => messagesWriter.AppendMessage(t),
            new UnhandledExceptionHandler(_ => {}),
            flowId
        );        
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            registeredTimeouts,
            initialMessages: [],
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);

        var task = messages.Take(2).ToList();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();
        var messageStore = functionStore.MessageStore;
        await messageStore.AppendMessage(
            storedId,
            new StoredMessage(JsonExtensions.ToJson("hello world").ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), "1")
        );
        await messageStore.AppendMessage(
            storedId,
            new StoredMessage(JsonExtensions.ToJson("hello world").ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), "1")
        );
        await messageStore.AppendMessage(
            storedId,
            new StoredMessage(JsonExtensions.ToJson("hello universe").ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );

        await task;
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await messageStore.GetMessages(storedId, skip: 0)).Count().ShouldBe(3);
    }
    
    public abstract Task MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations();
    protected async Task MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            storedId,  
            "humanInstanceId",
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var minimumTimeout = new FlowMinimumTimeout();
        using var registeredTimeouts = new FlowRegisteredTimeouts(
            CreateEffect(storedId, flowId, functionStore, minimumTimeout), 
            () => DateTime.UtcNow, 
            minimumTimeout, 
            t => messagesWriter.AppendMessage(t),
            new UnhandledExceptionHandler(_ => {}),
            flowId
        );        
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            new ExceptionThrowingEventSerializer(typeof(int)),
            registeredTimeouts,
            initialMessages: [],
            utcNow: () => DateTime.UtcNow
        );
        var messages = new Messages(messagesWriter, registeredTimeouts, messagesPullerAndEmitter, utcNow: () => DateTime.UtcNow);
        
        await messages.AppendMessage("hello world");
        await Should.ThrowAsync<MessageProcessingException>(messages.AppendMessage(1));
        await Should.ThrowAsync<MessageProcessingException>(() => messages.Skip(1).First());
        Should.Throw<MessageProcessingException>(() => messages.ToList());
    }
    
    public abstract Task BatchedMessagesIsDeliveredToAwaitingFlows();
    protected async Task BatchedMessagesIsDeliveredToAwaitingFlows(Task<IFunctionStore> functionStoreTask)
    {
        var flowType = TestFlowId.Create().Type;
        var functionStore = await functionStoreTask;
        using var registry = new FunctionsRegistry(functionStore);
        var registration = registry.RegisterParamless(
            flowType,
            async Task (workflow) => await workflow.Messages.FirstOfType<string>()
        );

        await registration.Schedule("Instance#1");
        await registration.Schedule("Instance#2");

        var controlPanel1 = await registration.ControlPanel("Instance#1").ShouldNotBeNullAsync();
        var controlPanel2 = await registration.ControlPanel("Instance#2").ShouldNotBeNullAsync();

        await controlPanel1.BusyWaitUntil(c => c.Status == Status.Suspended);
        await controlPanel2.BusyWaitUntil(c => c.Status == Status.Suspended);
        
        await registration.SendMessages(
            [
                new BatchedMessage("Instance#1", "hallo world", IdempotencyKey: "1"),
                new BatchedMessage("Instance#2", "hallo world", IdempotencyKey: "1")
            ]
        );
        
        await controlPanel1.BusyWaitUntil(c => c.Status == Status.Succeeded);
        await controlPanel2.BusyWaitUntil(c => c.Status == Status.Succeeded);
    }
    public abstract Task MultipleMessagesCanBeAppendedOneAfterTheOther();
    protected async Task MultipleMessagesCanBeAppendedOneAfterTheOther(Task<IFunctionStore> functionStoreTask)
    {
        var flowType = TestFlowId.Create().Type;
        var functionStore = await functionStoreTask;
        using var registry = new FunctionsRegistry(functionStore, new Settings(messagesDefaultMaxWaitForCompletion: TimeSpan.FromSeconds(10)));
        var messages = new List<string>();
        var registration = registry.RegisterParamless(
            flowType,
            async Task (workflow) =>
            {
                await foreach (var message in workflow.Messages)
                {
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
        await registration.SendMessage(instanceId, -1);

        var cp = await registration.ControlPanel(instanceId).ShouldNotBeNullAsync();
        await cp.WaitForCompletion();
        
        messages.Count.ShouldBe(4);
        messages[0].ShouldBe("Hallo");
        messages[1].ShouldBe("World");
        messages[2].ShouldBe("And");
        messages[3].ShouldBe("Universe");
    }

    private record Ping(int Number);
    private record Pong(int Number);
    
    public abstract Task PingPongMessagesCanBeExchangedMultipleTimes();
    protected async Task PingPongMessagesCanBeExchangedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var functionStore = await functionStoreTask;
        using var registry = new FunctionsRegistry(functionStore, new Settings(messagesDefaultMaxWaitForCompletion: TimeSpan.FromSeconds(1000), messagesPullFrequency: TimeSpan.FromMilliseconds(10)));
        ParamlessRegistration pongRegistration = null!;
        ParamlessRegistration pingRegistration = null!;
        
        pingRegistration = registry.RegisterParamless(
            "PingFlow",
            async Task (workflow) =>
            {
                for (var i = 0; i < 10; i++)
                {
                    await pongRegistration.SendMessage("Pong", new Ping(i), idempotencyKey:  $"Pong{i}");
                    await workflow.Messages.OfType<Pong>().Where(pong => pong.Number == i).First();
                }
                    
            });
        
        pongRegistration = registry.RegisterParamless(
            "PongFlow",
            async Task (workflow) =>
            {
                for (var i = 0; i < 10; i++)
                {
                    await workflow.Messages.OfType<Ping>().Where(pong => pong.Number == i).First();
                    await pingRegistration.SendMessage("Ping", new Pong(i), idempotencyKey:  $"Ping{i}");
                }
            });

        await pongRegistration.Schedule("Pong");
        await pingRegistration.Schedule("Ping");

        var pongCp = await pongRegistration.ControlPanel("Pong").ShouldNotBeNullAsync();
        var pingCp = await pingRegistration.ControlPanel("Ping").ShouldNotBeNullAsync();

        await pongCp.WaitForCompletion(allowPostponeAndSuspended: true);
        await pingCp.WaitForCompletion(allowPostponeAndSuspended: true);

        await pongCp.Refresh();
        await pongCp.Messages.Count.ShouldBeAsync(10);
        (await pongCp.Messages.AsObjects).OfType<Ping>().Count().ShouldBe(10);
        await pingCp.Refresh();
        await pingCp.Messages.Count.ShouldBeAsync(10);
        (await pingCp.Messages.AsObjects).OfType<Pong>().Count().ShouldBe(10);
    }
    
    public abstract Task NoOpMessageIsIgnored();
    protected async Task NoOpMessageIsIgnored(Task<IFunctionStore> functionStoreTask)
    {
        var flowType = TestFlowId.Create().Type;
        var functionStore = await functionStoreTask;
        using var registry = new FunctionsRegistry(functionStore);
        var registration = registry.RegisterFunc<string, string>(
            flowType,
            async Task<string> (_, workflow) => (await workflow.Messages.First(maxWait: TimeSpan.FromSeconds(10))).ToString()!
        );

        var invocation = registration.Invoke("SomeInstance", "SomeParam");

        await registration.SendMessage("SomeInstance", NoOp.Instance);
        await registration.SendMessage("SomeInstance", "Hallo World!");

        var result = await invocation;
        result.ShouldBe("Hallo World!");
    }

    private Effect CreateEffect(StoredId storedId, FlowId flowId, IFunctionStore functionStore, FlowMinimumTimeout flowMinimumTimeout)
    {
        var lazyExistingEffects = new Lazy<Task<IReadOnlyList<StoredEffect>>>(() => Task.FromResult((IReadOnlyList<StoredEffect>) new List<StoredEffect>()));
        var effectResults = new EffectResults(flowId, storedId, lazyExistingEffects, functionStore.EffectsStore, DefaultSerializer.Instance);
        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, flowMinimumTimeout);
        return effect;
    }
    
    private class ExceptionThrowingEventSerializer : ISerializer
    {
        private readonly Type _failDeserializationOnType;

        public ExceptionThrowingEventSerializer(Type failDeserializationOnType) 
            => _failDeserializationOnType = failDeserializationOnType;

        public byte[] Serialize<T>(T value) 
            => DefaultSerializer.Instance.Serialize(value);

        public byte[] Serialize(object? value, Type type) => DefaultSerializer.Instance.Serialize(value, type);

        public T Deserialize<T>(byte[] json)
            => DefaultSerializer.Instance.Deserialize<T>(json);

        public StoredException SerializeException(FatalWorkflowException exception)
            => DefaultSerializer.Instance.SerializeException(exception);
        public FatalWorkflowException DeserializeException(FlowId flowId, StoredException storedException)
            => DefaultSerializer.Instance.DeserializeException(flowId, storedException);

        public SerializedMessage SerializeMessage(object message, Type messageType)
            => DefaultSerializer.Instance.SerializeMessage(message, messageType);

        public object DeserializeMessage(byte[] json, byte[] type)
        {
            var eventType = Type.GetType(type.ToStringFromUtf8Bytes())!;
            if (eventType == _failDeserializationOnType)
                throw new Exception("Deserialization exception");

            return DefaultSerializer.Instance.DeserializeMessage(json, type);
        }
    }
}