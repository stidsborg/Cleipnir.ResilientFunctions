using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;
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
            parent: null
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
            new StoredMessage("hello world". ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );
        
        events = await messageStore.GetMessages(functionId, skip: 0);
        events.Count.ShouldBe(1);
        DefaultSerializer
            .Instance
            .DeserializeMessage(events[0].MessageJson, events[0].MessageType)
            .ShouldBe("hello world");
        
        events = await messageStore.GetMessages(functionId, skip: 1);
        events.ShouldBeEmpty();
        
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );
        
        events = await messageStore.GetMessages(functionId, skip: 1);
        events.Count.ShouldBe(1);
        
        DefaultSerializer
            .Instance
            .DeserializeMessage(events[0].MessageJson, events[0].MessageType)
            .ShouldBe("hello universe");
    }
}