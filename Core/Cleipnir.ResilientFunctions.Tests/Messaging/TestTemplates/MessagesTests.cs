using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
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
            parent: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue, 
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            initialMessages: []
        );
        var messages = new Messages(messagesWriter, messagesPullerAndEmitter);
        
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
        var functionStore = await functionStoreTask;

        using var registry = new FunctionsRegistry(functionStore);
        var registration = registry
            .RegisterFunc<string, EitherOrNone<string, int>>(flowId.Type, 
            (_, workflow) => workflow.Messages.FirstOfTypes<string, int>(expiresIn: TimeSpan.FromSeconds(10))
        );
        
        var scheduled = await registration.Schedule(flowId.Instance, param: "");
        await registration.SendMessage(flowId.Instance, "Hello");
        
        var eitherOrNone = await scheduled.Completion();
        eitherOrNone.HasFirst.ShouldBeTrue();
        eitherOrNone.First.ShouldBe("Hello");
        eitherOrNone.HasNone.ShouldBeFalse();
        eitherOrNone.HasSecond.ShouldBeFalse();
    }
    
    public abstract Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond();
    protected async Task MessagesFirstOfTypesReturnsSecondForFirstOfTypesOnSecond(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var functionStore = await functionStoreTask;

        using var registry = new FunctionsRegistry(functionStore);
        var registration = registry
            .RegisterFunc<string, EitherOrNone<string, int>>(flowId.Type, 
                (_, workflow) => workflow.Messages.FirstOfTypes<string, int>(expiresIn: TimeSpan.FromSeconds(10))
            );
        
        var scheduled = await registration.Schedule(flowId.Instance, param: "");
        await registration.SendMessage(flowId.Instance, 1);
        
        var eitherOrNone = await scheduled.Completion();
        eitherOrNone.HasSecond.ShouldBeTrue();
        eitherOrNone.Second.ShouldBe(1);
        eitherOrNone.HasNone.ShouldBeFalse();
        eitherOrNone.HasFirst.ShouldBeFalse();
    }
    
    public abstract Task MessagesFirstOfTypesReturnsNoneForTimeout();
    protected async Task MessagesFirstOfTypesReturnsNoneForTimeout(Task<IFunctionStore> functionStoreTask)
    {
        var flowId = TestFlowId.Create();
        var functionStore = await functionStoreTask;

        using var registry = new FunctionsRegistry(functionStore);
        var registration = registry
            .RegisterFunc<string, EitherOrNone<string, int>>(flowId.Type, 
                async (_, workflow) =>
                {
                     var result = await workflow.Messages.FirstOfTypes<string, int>(expiresIn: TimeSpan.FromMilliseconds(10));
                     return result;
                });
        
        var scheduled = await registration.Schedule(flowId.Instance, param: "");
        
        var eitherOrNone = await scheduled.Completion(maxWait: TimeSpan.FromSeconds(10));
        eitherOrNone.HasNone.ShouldBeTrue();
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
            parent: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            initialMessages: []
        );
        var messages = new Messages(messagesWriter, messagesPullerAndEmitter);

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
            parent: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            initialMessages: []
        );
        var messages = new Messages(messagesWriter, messagesPullerAndEmitter);
        
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
            parent: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            initialMessages: []
        );
        var messages = new Messages(messagesWriter, messagesPullerAndEmitter);

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
            parent: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            initialMessages: []
        );
        var messages = new Messages(messagesWriter, messagesPullerAndEmitter);
        
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
            parent: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            initialMessages: []
        );
        var messages = new Messages(messagesWriter, messagesPullerAndEmitter);

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
            parent: null
        );
        var messagesWriter = new MessageWriter(storedId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            defaultMaxWait: TimeSpan.MaxValue,
            isWorkflowRunning: () => true,
            functionStore,
            new ExceptionThrowingEventSerializer(typeof(int)),
            initialMessages: []
        );
        var messages = new Messages(messagesWriter, messagesPullerAndEmitter);
        
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

    private Effect CreateEffect(StoredId storedId, FlowId flowId, IFunctionStore functionStore)
    {
        var lazyExistingEffects = new Lazy<Task<IReadOnlyList<StoredEffect>>>(() => Task.FromResult((IReadOnlyList<StoredEffect>) new List<StoredEffect>()));
        var effectResults = new EffectResults(flowId, storedId, lazyExistingEffects, functionStore.EffectsStore, DefaultSerializer.Instance);
        var effect = new Effect(effectResults);
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