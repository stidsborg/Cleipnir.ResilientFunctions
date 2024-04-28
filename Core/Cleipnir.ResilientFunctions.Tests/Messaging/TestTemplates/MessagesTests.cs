using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
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
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var messagesWriter = new MessageWriter(functionId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var timeoutProvider = new TimeoutProvider(functionId, functionStore.TimeoutStore, messagesWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1));
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            timeoutProvider
        );
        var messages = new Messages(messagesWriter, timeoutProvider, messagesPullerAndEmitter);
        
        var task = messages.First();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await messages.AppendMessage("hello world");

        (await task).ShouldBe("hello world");
    }
    
    public abstract Task ExistingEventsShouldBeSameAsAllAfterEmit();
    protected async Task ExistingEventsShouldBeSameAsAllAfterEmit(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var messagesWriter = new MessageWriter(functionId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var timeoutProvider = new TimeoutProvider(functionId, functionStore.TimeoutStore, messagesWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1));
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            timeoutProvider
        );
        var messages = new Messages(messagesWriter, timeoutProvider, messagesPullerAndEmitter);

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
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var messagesWriter = new MessageWriter(functionId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var timeoutProvider = new TimeoutProvider(functionId, functionStore.TimeoutStore, messagesWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1));
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            timeoutProvider
        );
        var messages = new Messages(messagesWriter, timeoutProvider, messagesPullerAndEmitter);
        
        var task = messages.Take(2).ToList();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await messages.AppendMessage("hello world", idempotencyKey: "1");
        await messages.AppendMessage("hello world", idempotencyKey: "1");
        await messages.AppendMessage("hello universe");

        await BusyWait.UntilAsync(() => task.IsCompleted);
        task.IsCompletedSuccessfully.ShouldBeTrue();
        task.Result.Count.ShouldBe(2);
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await functionStore.MessageStore.GetMessages(functionId, skip: 0)).Count().ShouldBe(3);
    }
    
    public abstract Task MessagesBulkMethodOverloadAppendsAllEventsSuccessfully();
    protected async Task MessagesBulkMethodOverloadAppendsAllEventsSuccessfully(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var messagesWriter = new MessageWriter(functionId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var timeoutProvider = new TimeoutProvider(functionId, functionStore.TimeoutStore, messagesWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1));
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            timeoutProvider
        );
        var messages = new Messages(messagesWriter, timeoutProvider, messagesPullerAndEmitter);

        var task = messages.Take(2).ToList();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();
        await messages.AppendMessage("hello world", "1");
        await messages.AppendMessage("hello world", "1");
        await messages.AppendMessage("hello universe");

        await BusyWait.UntilAsync(() => task.IsCompletedSuccessfully);
        
        task.Result.Count.ShouldBe(2);
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await functionStore.MessageStore.GetMessages(functionId, skip: 0)).Count().ShouldBe(3);
    }

    public abstract Task MessagessSunshineScenarioUsingMessageStore();
    protected async Task MessagessSunshineScenarioUsingMessageStore(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var messagesWriter = new MessageWriter(functionId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var timeoutProvider = new TimeoutProvider(functionId, functionStore.TimeoutStore, messagesWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1));
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            timeoutProvider
        );
        var messages = new Messages(messagesWriter, timeoutProvider, messagesPullerAndEmitter);
        
        var task = messages.First();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();

        await functionStore.MessageStore.AppendMessage(
            functionId,
            new StoredMessage(JsonExtensions.ToJson("hello world"), typeof(string).SimpleQualifiedName())
        );

        (await task).ShouldBe("hello world");
    }

    public abstract Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore();
    protected async Task SecondEventWithExistingIdempotencyKeyIsIgnoredUsingMessageStore(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var messagesWriter = new MessageWriter(functionId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var timeoutProvider = new TimeoutProvider(functionId, functionStore.TimeoutStore, messagesWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1));
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            isWorkflowRunning: () => true,
            functionStore,
            DefaultSerializer.Instance,
            timeoutProvider
        );
        var messages = new Messages(messagesWriter, timeoutProvider, messagesPullerAndEmitter);

        var task = messages.Take(2).ToList();
        
        await Task.Delay(10);
        task.IsCompleted.ShouldBeFalse();
        var messageStore = functionStore.MessageStore;
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage(JsonExtensions.ToJson("hello world"), typeof(string).SimpleQualifiedName(), "1")
        );
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage(JsonExtensions.ToJson("hello world"), typeof(string).SimpleQualifiedName(), "1")
        );
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage(JsonExtensions.ToJson("hello universe"), typeof(string).SimpleQualifiedName())
        );

        await task;
        task.Result[0].ShouldBe("hello world");
        task.Result[1].ShouldBe("hello universe");
        
        (await messageStore.GetMessages(functionId, skip: 0)).Count().ShouldBe(3);
    }
    
    public abstract Task MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations();
    protected async Task MessagesRemembersPreviousThrownEventProcessingExceptionOnAllSubsequentInvocations(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var messagesWriter = new MessageWriter(functionId, functionStore, DefaultSerializer.Instance, scheduleReInvocation: (_, _) => Task.CompletedTask);
        var timeoutProvider = new TimeoutProvider(functionId, functionStore.TimeoutStore, messagesWriter, timeoutCheckFrequency: TimeSpan.FromSeconds(1));
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            defaultDelay: TimeSpan.FromMilliseconds(250),
            isWorkflowRunning: () => true,
            functionStore,
            new ExceptionThrowingEventSerializer(typeof(int)),
            timeoutProvider
        );
        var messages = new Messages(messagesWriter, timeoutProvider, messagesPullerAndEmitter);
        
        await messages.AppendMessage("hello world");
        await Should.ThrowAsync<MessageProcessingException>(messages.AppendMessage(1));
        await Should.ThrowAsync<MessageProcessingException>(() => messages.Skip(1).First());
        Should.Throw<MessageProcessingException>(() => messages.ToList());
    }
    
    private class ExceptionThrowingEventSerializer : ISerializer
    {
        private readonly Type _failDeserializationOnType;

        public ExceptionThrowingEventSerializer(Type failDeserializationOnType) 
            => _failDeserializationOnType = failDeserializationOnType;

        public StoredParameter SerializeParameter<TParam>(TParam parameter) where TParam : notnull 
            => DefaultSerializer.Instance.SerializeParameter(parameter);

        public TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull
            => DefaultSerializer.Instance.DeserializeParameter<TParam>(json, type);

        public StoredException SerializeException(Exception exception)
            => DefaultSerializer.Instance.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => DefaultSerializer.Instance.DeserializeException(storedException);

        public StoredResult SerializeResult<TResult>(TResult result)
            => DefaultSerializer.Instance.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type)
            => DefaultSerializer.Instance.DeserializeResult<TResult>(json, type);

        public JsonAndType SerializeMessage<TEvent>(TEvent message) where TEvent : notnull
            => DefaultSerializer.Instance.SerializeMessage(message);

        public object DeserializeMessage(string json, string type)
        {
            var eventType = Type.GetType(type)!;
            if (eventType == _failDeserializationOnType)
                throw new Exception("Deserialization exception");

            return DefaultSerializer.Instance.DeserializeMessage(json, type);
        }

        public string SerializeEffectResult<TResult>(TResult result)
            => DefaultSerializer.Instance.SerializeEffectResult(result);
        public TResult DeserializeEffectResult<TResult>(string json)
            => DefaultSerializer.Instance.DeserializeEffectResult<TResult>(json);

        public string SerializeState<TState>(TState state) where TState : WorkflowState, new()
            => DefaultSerializer.Instance.SerializeState(state);
        public TState DeserializeState<TState>(string json) where TState : WorkflowState, new()
            => DefaultSerializer.Instance.DeserializeState<TState>(json);
    }
}