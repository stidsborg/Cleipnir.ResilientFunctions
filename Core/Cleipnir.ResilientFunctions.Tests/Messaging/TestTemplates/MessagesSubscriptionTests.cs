using System;
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
                var message = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), (TimeSpan?)null);

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

                var message1 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), (TimeSpan?)null);
                await workflow.Delay(TimeSpan.FromMilliseconds(100));
                var message2 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), (TimeSpan?)null);
                await workflow.Delay(TimeSpan.FromMilliseconds(100));
                var message3 = await queueClient.Pull<string>(workflow, workflow.Effect.CreateNextImplicitId(), (TimeSpan?)null);
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
}