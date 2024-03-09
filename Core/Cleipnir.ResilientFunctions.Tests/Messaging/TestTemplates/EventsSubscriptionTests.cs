using System;
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

public abstract class MessagesSuscriptionTests
{
    public abstract Task EventsSubscriptionSunshineScenario();
    protected async Task EventsSubscriptionSunshineScenario(Task<IFunctionStore> functionStoreTask)
    {
        var functionId = TestFunctionId.Create();
        var functionStore = await functionStoreTask;
        await functionStore.CreateFunction(
            functionId, 
            Test.SimpleStoredParameter, 
            Test.SimpleStoredState, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var messageStore = functionStore.MessageStore;

        await messageStore.HasMoreMessages(functionId, skip: 0).ShouldBeFalseAsync();
        var events = await messageStore.GetMessages(functionId, skip: 0);
        events.ShouldBeEmpty();
        
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world". ToJson(), typeof(string).SimpleQualifiedName())
        );

        await messageStore.HasMoreMessages(functionId, skip: 0).ShouldBeTrueAsync();
        events = await messageStore.GetMessages(functionId, skip: 0);
        events.Count.ShouldBe(1);
        DefaultSerializer
            .Instance
            .DeserializeMessage(events[0].MessageJson, events[0].MessageType)
            .ShouldBe("hello world");
        
        await messageStore.HasMoreMessages(functionId, skip: 1).ShouldBeFalseAsync();
        events = await messageStore.GetMessages(functionId, skip: 1);
        events.ShouldBeEmpty();
        
        await messageStore.AppendMessage(
            functionId,
            new StoredMessage("hello universe".ToJson(), typeof(string).SimpleQualifiedName())
        );

        await messageStore.HasMoreMessages(functionId, skip: 0).ShouldBeTrueAsync();
        events = await messageStore.GetMessages(functionId, skip: 1);
        events.Count.ShouldBe(1);
        
        DefaultSerializer
            .Instance
            .DeserializeMessage(events[0].MessageJson, events[0].MessageType)
            .ShouldBe("hello universe");
    }
}