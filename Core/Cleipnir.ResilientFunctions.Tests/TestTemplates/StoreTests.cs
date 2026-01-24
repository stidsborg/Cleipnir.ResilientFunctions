using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities.Serialization;
using Shouldly;
using static Cleipnir.ResilientFunctions.Storage.CrudOperation;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StoreTests
{
    private const string PARAM = "param";

    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var storedParameter = paramJson;

        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration + 1;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: null,
            timestamp,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.StoredId.ShouldBe(functionId);
        storedFunction.Parameter.ShouldBe(paramJson.ToUtf8Bytes());
        storedFunction.Expires.ShouldBe(leaseExpiration);
        storedFunction.Timestamp.ShouldBe(timestamp);

        const string result = "hello world";
        var resultJson = result.ToJson();
        await store.SucceedFunction(
            functionId,
            result: resultJson.ToUtf8Bytes(),
            expectedReplica: ReplicaId.Empty,
            timestamp: DateTime.UtcNow.Ticks,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();
            
        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        var results = await store.GetResults([functionId]);
        var resultBytes = results[functionId];
        resultBytes.ShouldNotBeNull();
        resultBytes.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe(result);
    }
    
    public abstract Task NullParamScenarioTest();
    protected async Task NullParamScenarioTest(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration + 1;
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            param: null,
            leaseExpiration,
            postponeUntil: null,
            timestamp,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Parameter.ShouldBeNull();
    }
        
    public abstract Task BecomeLeaderSucceedsWhenEpochIsAsExpected();
    protected async Task BecomeLeaderSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var owner = ReplicaId.NewId();

        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            paramJson.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: 0,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store
            .RestartExecution(
                functionId,
                owner
            ).ShouldNotBeNullAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Expires.ShouldBe(0);
        storedFunction.OwnerId.ShouldBe(owner);
    }
        
    public abstract Task BecomeLeaderFailsWhenEpochIsNotAsExpected();
    protected async Task BecomeLeaderFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var owner = Guid.NewGuid().ToReplicaId();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration,
            postponeUntil: 0,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner
        ).ShouldNotBeNullAsync();
        
        await store
            .RestartExecution(
                functionId,
                owner: ReplicaId.NewId()
            ).ShouldBeNullAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Expires.ShouldBe(0);
        storedFunction.OwnerId.ShouldBe(owner);
    }

    public abstract Task CreatingTheSameFunctionTwiceReturnsFalse();
    protected async Task CreatingTheSameFunctionTwiceReturnsFalse(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            paramJson.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        ).ShouldBeNullAsync();
    }
    
    public abstract Task FunctionCreatedWithSendResultToReturnsSendResultToInStoredFunction();
    protected async Task FunctionCreatedWithSendResultToReturnsSendResultToInStoredFunction(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var sendResultToFunctionId = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            paramJson.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
    }
    
    public abstract Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut();
    protected async Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();

        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var nowTicks = DateTime.UtcNow.Ticks;

        var storedParameter = paramJson;
        
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();
        
        var postponedFunctions = await store.GetExpiredFunctions(expiresBefore: nowTicks - 100);
        postponedFunctions.ShouldBeEmpty();
    }
    
    public abstract Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut();
    protected async Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();

        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var nowTicks = DateTime.UtcNow.Ticks;

        var storedParameter = paramJson;
        
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();
        
        var postponedFunctions = await store.GetExpiredFunctions(expiresBefore: nowTicks + 100);
        postponedFunctions.Count().ShouldBe(1);
    }
    
    public abstract Task PostponeFunctionFailsWhenEpochIsNotAsExpected();
    protected async Task PostponeFunctionFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();

        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var nowTicks = DateTime.UtcNow.Ticks;

        var storedParameter = paramJson;
         
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.NewId(),
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeFalseAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Executing);
        ((string)DefaultSerializer.Instance
            .Deserialize(sf.Parameter!, typeof(string)))
            .ShouldBe(PARAM);
    }
    
    public abstract Task InitializeCanBeInvokedMultipleTimesSuccessfully();
    protected async Task InitializeCanBeInvokedMultipleTimesSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.Initialize();
        await store.Initialize();
    }
    
    public abstract Task OnlyEligibleCrashedFunctionsAreReturnedFromStore();
    protected async Task OnlyEligibleCrashedFunctionsAreReturnedFromStore(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var function1Id = TestStoredId.Create();
        var function2Id = StoredId.Create(function1Id.Type, Guid.NewGuid().ToString("N"));
        
        await store.CreateFunction(
            function1Id, 
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: 0,
            postponeUntil: 0,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        
        await store.CreateFunction(
            function2Id, 
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: 2,
            postponeUntil: 2,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        
        var storedFunctions = await store.GetExpiredFunctions(expiresBefore: 1);
        storedFunctions.Count.ShouldBe(1);
        var flowId = storedFunctions[0];
        flowId.ShouldBe(function1Id);
    }
    
    public abstract Task IncrementEpochSucceedsWhenEpochIsAsExpected();
    protected async Task IncrementEpochSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();

        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.RestartExecution(functionId, owner: ReplicaId.NewId()).ShouldNotBeNullAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
    }
    
    public abstract Task IncrementEpochFailsWhenEpochIsNotAsExpected();
    protected async Task IncrementEpochFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();

        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: Guid.NewGuid().ToReplicaId()
        ).ShouldNotBeNullAsync();

        await store.RestartExecution(functionId, owner: ReplicaId.NewId()).ShouldBeNullAsync();
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
    }
    
    public abstract Task DeletingExistingFunctionSucceeds();
    public async Task DeletingExistingFunctionSucceeds(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson();
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));
        
        var success = await store.DeleteFunction(functionId);
        success.ShouldBeTrue();
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldBeNull();

        success = await store.DeleteFunction(functionId);
        success.ShouldBeFalse();
    }

    public abstract Task FailFunctionSucceedsWhenEpochIsAsExpected();
    public async Task FailFunctionSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        var storedId = flowId.ToStoredId(new StoredType(1));

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            storedId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        var storedException = new StoredException(
            ExceptionMessage: "Something went wrong",
            ExceptionStackTrace: "StackTrace",
            ExceptionType: typeof(Exception).SimpleQualifiedName()
        );
        
        await store.FailFunction(
            storedId,
            storedException,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        );

        await BusyWait.Until(() => store.GetFunction(storedId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(storedId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Failed);
        sf.Exception.ShouldNotBeNull();
        var fatalWorkflowException = DefaultSerializer.Instance.DeserializeException(flowId, sf.Exception);
        fatalWorkflowException.FlowErrorMessage.ShouldBe(storedException.ExceptionMessage);
        fatalWorkflowException.FlowStackTrace.ShouldBe(storedException.ExceptionStackTrace);
        fatalWorkflowException.ErrorType.ShouldBe(typeof(Exception));
    }
    
    public abstract Task SetFunctionStateSucceedsWhenEpochIsAsExpected();
    public async Task SetFunctionStateSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson().ToUtf8Bytes();
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            "completed".ToJson().ToUtf8Bytes(),
            storedException: null,
            expires: DateTime.UtcNow.Ticks,
            expectedReplica: null
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        sf.Exception.ShouldBeNull();
    }
    
    public abstract Task SetFunctionStateSucceedsWithMessagesWhenEpochIsAsExpected();
    public async Task SetFunctionStateSucceedsWithMessagesWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var messages = store.MessageStore;
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson();
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var message1 = new StoredMessage(
            "hello everyone".ToJson().ToUtf8Bytes(),
            MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
            Position: 0,
            IdempotencyKey: "idempotency_key_1"
        );
        await messages.AppendMessage(functionId, message1);

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter.ToUtf8Bytes(),
            "completed".ToJson().ToUtf8Bytes(),
            storedException: null,
            expires: DateTime.Now.Ticks,
            expectedReplica: null
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        sf.Exception.ShouldBeNull();

        var storedMessages = await store.MessageStore.GetMessages(functionId, skip: 0);
        storedMessages.Count.ShouldBe(1);
        var deserializedMessage = (string) DefaultSerializer.Instance.DeserializeMessage(storedMessages[0].MessageContent, storedMessages[0].MessageType);
        deserializedMessage.ShouldBe("hello everyone");
    }
    
    public abstract Task ExecutingFunctionCanBeSuspendedSuccessfully();
    public async Task ExecutingFunctionCanBeSuspendedSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.SuspendFunction(
            functionId,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeAsync(true);

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Suspended);
        sf.Parameter.ShouldBe(storedParameter.ToUtf8Bytes());

        var messages = await store.MessageStore.GetMessages(functionId, skip: 0);
        messages.ShouldBeEmpty();

        await Task.Delay(500);

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );
    }
    
    public abstract Task FunctionStatusForNonExistingFunctionIsNull();
    public async Task FunctionStatusForNonExistingFunctionIsNull(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        
        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );
    }
    
    public abstract Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch();
    public async Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson();
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.RestartExecution(
            functionId,
            owner: ReplicaId.NewId()
        ).ShouldNotBeNullAsync();
        await store.RestartExecution(
            functionId, 
            owner: ReplicaId.NewId()
        ).ShouldBeNullAsync();
    }
    
    public abstract Task RestartingFunctionShouldSetInterruptedToFalse();
    public async Task RestartingFunctionShouldSetInterruptedToFalse(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson();
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.Interrupt(functionId).ShouldBeTrueAsync();
        await store.Interrupted(functionId).ShouldBeAsync(true);

        await store.RestartExecution(
            functionId, 
            owner: ReplicaId.NewId()
        ).ShouldNotBeNullAsync();
       
        await store.Interrupted(functionId).ShouldBeAsync(false);
    }
    
    public abstract Task MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated();
    public async Task MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson();
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.MessageStore.AppendMessage(functionId, new StoredMessage("Hello".ToJson().ToUtf8Bytes(), MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0));
        await store.MessageStore.AppendMessage(functionId, new StoredMessage("World".ToJson().ToUtf8Bytes(), MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0));
        
        var messages = await store.MessageStore.GetMessages(functionId, skip: 0);
        messages.Count.ShouldBe(2);
        messages[0].DefaultDeserialize().ShouldBe("Hello");
        messages[1].DefaultDeserialize().ShouldBe("World");
    }
    
    public abstract Task FunctionStatusAndEpochCanBeSuccessfullyFetched();
    public async Task FunctionStatusAndEpochCanBeSuccessfullyFetched(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson();
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter.ToUtf8Bytes(),
            "completed".ToJson().ToUtf8Bytes(),
            storedException: null,
            expires: DateTime.Now.Ticks,
            expectedReplica: null
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var status = await store.GetFunctionStatus(functionId);
        status.ShouldBe(Status.Succeeded);
    }
    
    public abstract Task EpochIsNotIncrementedOnCompletion();
    protected async Task EpochIsNotIncrementedOnCompletion(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.SucceedFunction(
            functionId,
            result: null,
            DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
    }
    
    public abstract Task EpochIsNotIncrementedOnPostponed();
    protected async Task EpochIsNotIncrementedOnPostponed(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
    }
    
    public abstract Task EpochIsNotIncrementedOnFailure();
    protected async Task EpochIsNotIncrementedOnFailure(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.FailFunction(
            functionId,
            new StoredException("ExceptionMessage", ExceptionStackTrace: null, typeof(Exception).SimpleQualifiedName()),
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
    }
    
    public abstract Task EpochIsNotIncrementedOnSuspension();
    protected async Task EpochIsNotIncrementedOnSuspension(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.SuspendFunction(
            functionId,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
    }
    
    public abstract Task SuspensionDoesNotSucceedOnExpectedMessagesCountMismatchButPostponesFunction();
    protected async Task SuspensionDoesNotSucceedOnExpectedMessagesCountMismatchButPostponesFunction(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("some message".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        await store.Interrupt(functionId);
        
        await store.SuspendFunction(
            functionId,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Postponed);
        storedFunction.Expires.ShouldBe(0);
    }
    
    public abstract Task FunctionIsStillExecutingOnSuspensionAndInterruptCountMismatch();
    protected async Task FunctionIsStillExecutingOnSuspensionAndInterruptCountMismatch(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes(), Position: 0)
        );

        await store.Interrupt(functionId);
        
        var success = await store.SuspendFunction(
            functionId,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        );
        
        success.ShouldBeTrue();
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Postponed);
        storedFunction.Expires.ShouldBe(0);
    }
    
    public abstract Task InterruptCountCanBeIncrementedForExecutingFunction();
    protected async Task InterruptCountCanBeIncrementedForExecutingFunction(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Interrupted.ShouldBeFalse();
        
        await store.Interrupt(functionId);
        
        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Interrupted.ShouldBeTrue();
        storedFunction.Status.ShouldBe(Status.Executing);
    }
    
    public abstract Task NonExecutingFunctionCanBeInterrupted();
    protected async Task NonExecutingFunctionCanBeInterrupted(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.SuspendFunction(
            functionId,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Interrupted.ShouldBeFalse();
        storedFunction.Status.ShouldBe(Status.Suspended);
        
        await store.Interrupt(functionId).ShouldBeTrueAsync();

        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Interrupted.ShouldBeTrue();
        storedFunction.Status.ShouldBe(Status.Postponed);
    }
    
    public abstract Task InterruptCountForNonExistingFunctionIsNull();
    protected async Task InterruptCountForNonExistingFunctionIsNull(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var store = await storeTask;
        (await store.Interrupted(functionId)).ShouldBeNull();
    }
    
    public abstract Task DefaultStateCanSetAndFetchedAfterwards();
    protected async Task DefaultStateCanSetAndFetchedAfterwards(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var effectsStore = store.EffectsStore;
        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await effectsStore.SetEffectResult(functionId, new StoredEffect(0.ToEffectId(), WorkStatus.Completed, "some default state".ToUtf8Bytes(), StoredException: null, Alias: null).ToStoredChange(functionId, Insert), session: null);

        var storedEffects = await effectsStore.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(1);
        storedEffects.Single().EffectId.ShouldBe(0.ToEffectId());
        storedEffects.Single().Result.ShouldBe("some default state".ToUtf8Bytes());
    }
    
    public abstract Task SucceededFunctionsCanBeFetchedSuccessfully();
    protected async Task SucceededFunctionsCanBeFetchedSuccessfully(Task<IFunctionStore> storeTask)
    {
        var functionId1 = TestStoredId.Create();
        var functionId2 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());
        var functionId3 = TestStoredId.Create();
        var store = await storeTask;

        async Task CreateAndSucceedFunction(StoredId functionId, long timestamp)
        {
            await store.CreateFunction(
                functionId, 
                "humanInstanceId",
                param: Test.SimpleStoredParameter,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: timestamp,
                parent: null,
                owner: ReplicaId.Empty
            ).ShouldNotBeNullAsync();

            await store.SucceedFunction(
                functionId,
                result: null,
                timestamp: timestamp,
                expectedReplica: ReplicaId.Empty,
                effects: null,
                messages: null,
                storageSession: null
            ).ShouldBeTrueAsync();
        }

        await CreateAndSucceedFunction(functionId1, timestamp: 1);
        await CreateAndSucceedFunction(functionId2, timestamp: 3);
        await CreateAndSucceedFunction(functionId3, timestamp: 0);

        var succeededFunctions = await store.GetSucceededFunctions(completedBefore: 2);
        succeededFunctions.Count.ShouldBe(2);
        succeededFunctions.Any(id => id == functionId1).ShouldBeTrue();
        succeededFunctions.Any(id => id == functionId3).ShouldBeTrue();
    }
    
    public abstract Task BulkScheduleInsertsAllFunctionsSuccessfully();
    protected async Task BulkScheduleInsertsAllFunctionsSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parent = TestStoredId.Create();
        var typeId = TestStoredId.Create().Type;
        var functionIds = Enumerable
            .Range(0, 500)
            .Select(i => StoredId.Create(typeId, i.ToString()))
            .ToList();

        var insertedCount = await store.BulkScheduleFunctions(
            functionIds.Select(functionId => new IdWithParam(functionId, "humanInstanceId", Param: functionId.ToString().ToUtf8Bytes())),
            parent
        );

        insertedCount.ShouldBe(functionIds.Count);

        var eligibleFunctions =
            await store.GetExpiredFunctions(DateTime.UtcNow.Ticks);

        eligibleFunctions.Count.ShouldBe(functionIds.Count);
        foreach (var flowId in functionIds)
        {
            eligibleFunctions.Any(f => f == flowId).ShouldBeTrue();
        }

        foreach (var id in functionIds)
        {
            var sf = await store.GetFunction(id);
            sf.ShouldNotBeNull();
            sf.Parameter!.ToStringFromUtf8Bytes().ShouldBe(id.ToString());
            sf.ParentId.ShouldBe(parent);
        }
    }

    public abstract Task BulkScheduleDoesNotCountExistingFunctions();
    protected async Task BulkScheduleDoesNotCountExistingFunctions(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parent = TestStoredId.Create();
        var typeId = TestStoredId.Create().Type;

        // Create 10 function IDs
        var functionIds = Enumerable
            .Range(0, 10)
            .Select(i => StoredId.Create(typeId, i.ToString()))
            .ToList();

        // First bulk insert: insert all 10 functions
        var firstInsertCount = await store.BulkScheduleFunctions(
            functionIds.Select(functionId => new IdWithParam(functionId, "humanInstanceId", Param: functionId.ToString().ToUtf8Bytes())),
            parent
        );

        firstInsertCount.ShouldBe(10);

        // Second bulk insert: try to insert the same 10 functions again, plus 5 new ones
        var newFunctionIds = Enumerable
            .Range(10, 5)
            .Select(i => StoredId.Create(typeId, i.ToString()))
            .ToList();

        var allFunctionIds = functionIds.Concat(newFunctionIds).ToList();

        var secondInsertCount = await store.BulkScheduleFunctions(
            allFunctionIds.Select(functionId => new IdWithParam(functionId, "humanInstanceId", Param: functionId.ToString().ToUtf8Bytes())),
            parent
        );

        // Should only count the 5 new functions, not the 10 duplicates
        secondInsertCount.ShouldBe(5);

        // Verify all 15 functions exist
        var eligibleFunctions = await store.GetExpiredFunctions(DateTime.UtcNow.Ticks);
        eligibleFunctions.Count.ShouldBe(15);

        foreach (var id in allFunctionIds)
        {
            var sf = await store.GetFunction(id);
            sf.ShouldNotBeNull();
        }
    }

    public abstract Task BulkScheduleWithEmptyCollectionReturnsZero();
    protected async Task BulkScheduleWithEmptyCollectionReturnsZero(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var parent = TestStoredId.Create();

        // Call BulkScheduleFunctions with empty collection
        var insertedCount = await store.BulkScheduleFunctions(
            Enumerable.Empty<IdWithParam>(),
            parent
        );

        // Should return 0 for empty collection
        insertedCount.ShouldBe(0);
    }

    public abstract Task DifferentTypesAreFetchedByGetExpiredFunctionsCall();
    protected async Task DifferentTypesAreFetchedByGetExpiredFunctionsCall(Task<IFunctionStore> storeTask)
    {
        var flowId1 = TestStoredId.Create();
        var flowId2 = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var storedParameter = paramJson;

        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration + 1;
        var session = await store.CreateFunction(
            flowId1,
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: 0,
            timestamp,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();
        session = await store.CreateFunction(
            flowId2,
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: 0,
            timestamp,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();
        
        var expires = await store.GetExpiredFunctions(expiresBefore: DateTime.UtcNow.Ticks);
        
        expires.Count.ShouldBe(2);
        var flowIds = expires.Select(x => x).ToHashSet();
        flowIds.Contains(flowId1).ShouldBeTrue();
        flowIds.Contains(flowId2).ShouldBeTrue();
    }
    
    public abstract Task TypeStoreSunshineScenarioTest();
    protected async Task TypeStoreSunshineScenarioTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask.SelectAsync(store => store.TypeStore);

        var types = await store.GetAllFlowTypes();
        types.Count.ShouldBe(0);

        var flow1Value = await store.InsertOrGetStoredType("TestFlow1");
        var value2 = await store.InsertOrGetStoredType("TestFlow1");
        flow1Value.ShouldBe(value2);

        types = await store.GetAllFlowTypes();
        types.Count.ShouldBe(1);
        types.TryGetValue("TestFlow1", out var dictValue).ShouldBeTrue();
        dictValue.ShouldBe(flow1Value);

        var flow2Value = await store.InsertOrGetStoredType("TestFlow2");
        flow2Value.ShouldNotBe(flow1Value);
        
        types = await store.GetAllFlowTypes();
        types.Count.ShouldBe(2);
        types.TryGetValue("TestFlow1", out dictValue).ShouldBeTrue();
        dictValue.ShouldBe(flow1Value);
        types.TryGetValue("TestFlow2", out dictValue).ShouldBeTrue();
        dictValue.ShouldBe(flow2Value);
    }
    
    public abstract Task FlowWithParentIsReturnedInSubsequentGetTest();
    protected async Task FlowWithParentIsReturnedInSubsequentGetTest(Task<IFunctionStore> storeTask)
    {
        var id = TestStoredId.Create();
        var parentId = TestStoredId.Create();
        var store = await storeTask;
        var session = await store.CreateFunction(
            id,
            humanInstanceId: "SomeInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parentId,
            owner: null
        );
        session.ShouldBeNull();

        var sf = await store.GetFunction(id).ShouldNotBeNullAsync();
        sf.ParentId.ShouldBe(parentId);
    }
    
    public abstract Task FlowWithWithoutParentIsReturnsNullParentInSubsequentGetTest();
    protected async Task FlowWithWithoutParentIsReturnsNullParentInSubsequentGetTest(Task<IFunctionStore> storeTask)
    {
        var id = TestStoredId.Create();
        var store = await storeTask;
        var session = await store.CreateFunction(
            id,
            humanInstanceId: "SomeInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var sf = await store.GetFunction(id).ShouldNotBeNullAsync();
        sf.ParentId.ShouldBeNull();
    }
    
    public abstract Task MultipleFunctionsStatusCanBeFetched();
    protected async Task MultipleFunctionsStatusCanBeFetched(Task<IFunctionStore> storeTask)
    {
        var functionId1 = TestStoredId.Create();
        var functionId2 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());
        var functionId3 = TestStoredId.Create();
        var store = await storeTask;

        async Task CreateAndSucceedFunction(StoredId functionId, long timestamp)
        {
            await store.CreateFunction(
                functionId, 
                "humanInstanceId",
                param: Test.SimpleStoredParameter,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: timestamp,
                parent: null,
                owner: ReplicaId.Empty
            ).ShouldNotBeNullAsync();

            await store.SucceedFunction(
                functionId,
                result: null,
                timestamp: timestamp,
                expectedReplica: ReplicaId.Empty,
                effects: null,
                messages: null,
                storageSession: null
            ).ShouldBeTrueAsync();
        }

        await CreateAndSucceedFunction(functionId1, timestamp: 1);
        await CreateAndSucceedFunction(functionId2, timestamp: 3);
        await CreateAndSucceedFunction(functionId3, timestamp: 0);

        var statuses = await store.GetFunctionsStatus([functionId1, functionId3]);
        var statusAndEpoch1 = statuses.Single(s => s.StoredId == functionId1);
        statusAndEpoch1.Status.ShouldBe(Status.Succeeded);
        
        var statusAndEpoch2 = statuses.Single(s => s.StoredId == functionId1);
        statusAndEpoch2.Status.ShouldBe(Status.Succeeded);
    }
    
    public abstract Task InterruptedFunctionIsNotPostponedToZeroWhenInterrupted();
    protected async Task InterruptedFunctionIsNotPostponedToZeroWhenInterrupted(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        var store = await storeTask;

        await store.CreateFunction(
            storedId,
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.Interrupt([storedId]);

        var success = await store.PostponeFunction(
            storedId,
            postponeUntil: 0,
            timestamp: 100,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        );
        success.ShouldBeTrue();

        var sf = await store.GetFunction(storedId).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Postponed);
        sf.Expires.ShouldBe(0);
    }

    public abstract Task InterruptNothingWorks();
    protected async Task InterruptNothingWorks(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.Interrupt(storedIds: []);
    }
    
    public abstract Task InterruptedFunctionIsPostponedWhenIgnoringInterruptedFunction();
    protected async Task InterruptedFunctionIsPostponedWhenIgnoringInterruptedFunction(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        var store = await storeTask;

        await store.CreateFunction(
            storedId,
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.Interrupt([storedId]);

        var success = await store.PostponeFunction(
            storedId,
            postponeUntil: 0,
            timestamp: 0,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        );
        success.ShouldBeTrue();

        var sf = await store.GetFunction(storedId).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Postponed);
    }
    
    public abstract Task FunctionCanBeCreatedWithMessagesAndEffects();
    protected async Task FunctionCanBeCreatedWithMessagesAndEffects(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var effectId1 = new EffectId(["SomeEffect1".GetHashCode()]);
        var effect1 = new StoredEffect(
            effectId1,
            WorkStatus.Completed,
            Result: "hello world".ToUtf8Bytes(),
            StoredException: null,
            Alias: null
        );
        var effectId2 = new EffectId(["SomeEffect2".GetHashCode()]);
        var effect2 = new StoredEffect(
            effectId2,
            WorkStatus.Completed,
            Result: "hello universe".ToUtf8Bytes(),
            StoredException: null,
            Alias: null
        );

        var message1 = new StoredMessage(
            MessageContent: "hallo world".ToUtf8Bytes(),
            MessageType: "some type".ToUtf8Bytes(),
            Position: 0,
            IdempotencyKey: "some idempotency key"
        );
        var message2 = new StoredMessage(
            MessageContent: "hallo universe".ToUtf8Bytes(),
            MessageType: "some type".ToUtf8Bytes(),
            Position: 0,
            IdempotencyKey: "some idempotency key"
        );

        var session = await store.CreateFunction(
            storedId,
            "humanInstanceId",
            paramJson.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            effects: [effect1, effect2],
            messages: [message1, message2],
            owner: null
        );
        session.ShouldBeNull();

        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        effectResults.Count.ShouldBe(2);
        var effectResult1 = effectResults.Single(r => r.EffectId.Id == "SomeEffect1".GetHashCode());
        effectResult1.EffectId.ShouldBe(new EffectId(["SomeEffect1".GetHashCode()]));
        effectResult1.Result!.ToStringFromUtf8Bytes().ShouldBe("hello world");
        var effectResult2 = effectResults.Single(r => r.EffectId.Id == "SomeEffect2".GetHashCode());
        effectResult2.EffectId.ShouldBe(new EffectId(["SomeEffect2".GetHashCode()]));
        effectResult2.Result!.ToStringFromUtf8Bytes().ShouldBe("hello universe");

        var messages = await store.MessageStore.GetMessages(storedId, skip: 0);
        messages.Count.ShouldBe(2);
        var fetchedMessage1 = messages[0];
        fetchedMessage1.MessageType.ToStringFromUtf8Bytes().ShouldBe("some type");
        fetchedMessage1.MessageContent.ToStringFromUtf8Bytes().ShouldBe("hallo world");
        fetchedMessage1.IdempotencyKey.ShouldBe("some idempotency key");

        var fetchedMessage2 = messages[1];
        fetchedMessage2.MessageType.ToStringFromUtf8Bytes().ShouldBe("some type");
        fetchedMessage2.MessageContent.ToStringFromUtf8Bytes().ShouldBe("hallo universe");
        fetchedMessage2.IdempotencyKey.ShouldBe("some idempotency key");
        
        //idempotency check
        await store.CreateFunction(
            storedId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            effects: [effect1, effect2],
            messages: [message1, message2],
            owner: null
        ).ShouldBeNullAsync();
    }
    
    public abstract Task FunctionCanBeCreatedWithMessagesOnly();
    protected async Task FunctionCanBeCreatedWithMessagesOnly(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var message1 = new StoredMessage(
            MessageContent: "hallo world".ToUtf8Bytes(),
            MessageType: "some type".ToUtf8Bytes(),
            Position: 0,
            IdempotencyKey: "some idempotency key"
        );
        var message2 = new StoredMessage(
            MessageContent: "hallo universe".ToUtf8Bytes(),
            MessageType: "some type".ToUtf8Bytes(),
            Position: 0,
            IdempotencyKey: "some idempotency key"
        );

        var session = await store.CreateFunction(
            storedId,
            "humanInstanceId",
            paramJson.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            effects: null,
            messages: [message1, message2],
            owner: null
        );
        session.ShouldBeNull();

        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        effectResults.Count.ShouldBe(0);

        var messages = await store.MessageStore.GetMessages(storedId, skip: 0);
        messages.Count.ShouldBe(2);
        var fetchedMessage1 = messages[0];
        fetchedMessage1.MessageType.ToStringFromUtf8Bytes().ShouldBe("some type");
        fetchedMessage1.MessageContent.ToStringFromUtf8Bytes().ShouldBe("hallo world");
        fetchedMessage1.IdempotencyKey.ShouldBe("some idempotency key");

        var fetchedMessage2 = messages[1];
        fetchedMessage2.MessageType.ToStringFromUtf8Bytes().ShouldBe("some type");
        fetchedMessage2.MessageContent.ToStringFromUtf8Bytes().ShouldBe("hallo universe");
        fetchedMessage2.IdempotencyKey.ShouldBe("some idempotency key");
        
        //idempotency check
        await store.CreateFunction(
            storedId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            effects: null,
            messages: [message1, message2],
            owner: null
        ).ShouldBeNullAsync();
    }
    
    public abstract Task FunctionCanBeCreatedWithEffectsOnly();
    protected async Task FunctionCanBeCreatedWithEffectsOnly(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var effectId1 = new EffectId(["SomeEffect1".GetHashCode()]);
        var effect1 = new StoredEffect(
            effectId1,
            WorkStatus.Completed,
            Result: "hello world".ToUtf8Bytes(),
            StoredException: null,
            Alias: null
        );
        var effectId2 = new EffectId(["SomeEffect2".GetHashCode()]);
        var effect2 = new StoredEffect(
            effectId2,
            WorkStatus.Completed,
            Result: "hello universe".ToUtf8Bytes(),
            StoredException: null,
            Alias: null
        );

        var session = await store.CreateFunction(
            storedId,
            "humanInstanceId",
            paramJson.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            effects: [effect1, effect2],
            messages: null,
            owner: null
        );
        session.ShouldBeNull();

        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        effectResults.Count.ShouldBe(2);
        var effectResult1 = effectResults.Single(r => r.EffectId.Id == "SomeEffect1".GetHashCode());
        effectResult1.EffectId.ShouldBe(new EffectId(["SomeEffect1".GetHashCode()]));
        effectResult1.Result!.ToStringFromUtf8Bytes().ShouldBe("hello world");
        var effectResult2 = effectResults.Single(r => r.EffectId.Id == "SomeEffect2".GetHashCode());
        effectResult2.EffectId.ShouldBe(new EffectId(["SomeEffect2".GetHashCode()]));
        effectResult2.Result!.ToStringFromUtf8Bytes().ShouldBe("hello universe");

        var messages = await store.MessageStore.GetMessages(storedId, skip: 0);
        messages.Count.ShouldBe(0);
        
        //idempotency check
        await store.CreateFunction(
            storedId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            effects: [effect1, effect2],
            messages: null,
            owner: null
        ).ShouldBeNullAsync();
    }
    
    public abstract Task RestartExecutionReturnsEffectsAndMessages();
    protected async Task RestartExecutionReturnsEffectsAndMessages(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            paramJson.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage(
                "hallo message".ToUtf8Bytes(),
                typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
                Position: 0
            )
        );

        await store.EffectsStore.SetEffectResult(
            functionId,
            new StoredEffect(
                "Test".GetHashCode().ToEffectId(),
                WorkStatus.Completed,
                "hallo effect".ToUtf8Bytes(),
                StoredException: null,
                Alias: null
                ).ToStoredChange(functionId, Insert),
            session: null
        );

        var (sf, effects, messages, _) = await store
            .RestartExecution(
                functionId,
                owner: ReplicaId.NewId()
            ).ShouldNotBeNullAsync();

        sf.StoredId.ShouldBe(functionId);
        effects.Count.ShouldBe(1);
        effects.Single().EffectId.Id.ShouldBe("Test".GetHashCode());
        effects.Single().Result!.ToStringFromUtf8Bytes().ShouldBe("hallo effect");
        messages.Count.ShouldBe(1);
        messages.Single().MessageContent.ToStringFromUtf8Bytes().ShouldBe("hallo message");
    }
    
    public abstract Task RestartExecutionWorksWithEmptyEffectsAndMessages();
    protected async Task RestartExecutionWorksWithEmptyEffectsAndMessages(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var owner = ReplicaId.NewId();

        var session = await store.CreateFunction(
            functionId,
            "humanInstanceId",
            paramJson.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        var (sf, effects, messages, _) = await store
            .RestartExecution(
                functionId,
                owner
            ).ShouldNotBeNullAsync();

        sf.StoredId.ShouldBe(functionId);
        effects.Count.ShouldBe(0);
        messages.Count.ShouldBe(0);
        sf.OwnerId.ShouldBe(owner);
    }
    
    public abstract Task FunctionOwnedByReplicaIsPostponedAfterRescheduleFunctionsInvocation();
    protected async Task FunctionOwnedByReplicaIsPostponedAfterRescheduleFunctionsInvocation(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var replicaId1 = ReplicaId.NewId();
        var replicaId2 = ReplicaId.NewId();
        var storedId1 = TestStoredId.Create();
        await store.CreateFunction(
            storedId1,
            humanInstanceId: "SomeInstanceId",
            param: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: replicaId1
        ).ShouldNotBeNullAsync();
        var storedId2 = TestStoredId.Create();
        await store.CreateFunction(
            storedId2,
            humanInstanceId: "SomeInstanceId1",
            param: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: replicaId1
        ).ShouldNotBeNullAsync();
        var storedId3 = TestStoredId.Create();
        await store.CreateFunction(
            storedId3,
            humanInstanceId: "SomeInstanceId",
            param: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: replicaId2
        ).ShouldNotBeNullAsync();

        var replicas = await store.GetOwnerReplicas();
        replicas.Count.ShouldBe(2);
        replicas.Any(r => r == replicaId1).ShouldBeTrue();
        replicas.Any(r => r == replicaId2).ShouldBeTrue();
        
        await store.RescheduleCrashedFunctions(replicaId1);
        
        var sf1 = await store.GetFunction(storedId1).ShouldNotBeNullAsync();
        sf1.Status.ShouldBe(Status.Postponed);
        sf1.OwnerId.ShouldBeNull();
        sf1.Expires.ShouldBe(0);
        
        var sf2 = await store.GetFunction(storedId2).ShouldNotBeNullAsync();
        sf2.Status.ShouldBe(Status.Postponed);
        sf2.OwnerId.ShouldBeNull();
        sf2.Expires.ShouldBe(0);
        
        var sf3 = await store.GetFunction(storedId3).ShouldNotBeNullAsync();
        sf3.Status.ShouldBe(Status.Executing);
        sf3.OwnerId.ShouldBe(replicaId2);
        
        replicas = await store.GetOwnerReplicas();
        replicas.Count.ShouldBe(1);
        replicas.Any(r => r == replicaId2).ShouldBeTrue();
    }
    
    public abstract Task SuspensionSetsOwnerToNull();
    protected async Task SuspensionSetsOwnerToNull(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.SuspendFunction(
            functionId,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.OwnerId.ShouldBeNull();
    }
    
    public abstract Task FailureSetsOwnerToNull();
    protected async Task FailureSetsOwnerToNull(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.FailFunction(
            functionId,
            storedException: new StoredException("SomeMessage", ExceptionStackTrace: null, "SomeExceptionType"),
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.OwnerId.ShouldBeNull();
    }
    
    public abstract Task PostponedSetsOwnerToNull();
    protected async Task PostponedSetsOwnerToNull(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.OwnerId.ShouldBeNull();
    }
    
    public abstract Task SucceedSetsOwnerToNull();
    protected async Task SucceedSetsOwnerToNull(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.SucceedFunction(
            functionId,
            result: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.OwnerId.ShouldBeNull();
    }

    public abstract Task GetInterruptedFunctionsReturnsOnlyInterruptedFunctions();
    protected async Task GetInterruptedFunctionsReturnsOnlyInterruptedFunctions(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId1 = TestStoredId.Create();
        var functionId2 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());
        var functionId3 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());
        var functionId4 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());

        // Create 4 functions
        var session = await store.CreateFunction(
            functionId1,
            "humanInstanceId1",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        session = await store.CreateFunction(
            functionId2,
            "humanInstanceId2",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        session = await store.CreateFunction(
            functionId3,
            "humanInstanceId3",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        session = await store.CreateFunction(
            functionId4,
            "humanInstanceId4",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        // Interrupt functions 1 and 3
        await store.Interrupt(functionId1).ShouldBeTrueAsync();
        await store.Interrupt(functionId3).ShouldBeTrueAsync();

        // Get interrupted functions from the set of all 4 functions
        var interruptedFunctions = await store.GetInterruptedFunctions([functionId1, functionId2, functionId3, functionId4]);

        // Should return only the 2 interrupted functions
        interruptedFunctions.Count.ShouldBe(2);
        interruptedFunctions.Any(id => id == functionId1).ShouldBeTrue();
        interruptedFunctions.Any(id => id == functionId3).ShouldBeTrue();
        interruptedFunctions.Any(id => id == functionId2).ShouldBeFalse();
        interruptedFunctions.Any(id => id == functionId4).ShouldBeFalse();
    }

    public abstract Task GetInterruptedFunctionsReturnsEmptyListWhenNoIdsProvided();
    protected async Task GetInterruptedFunctionsReturnsEmptyListWhenNoIdsProvided(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;

        var interruptedFunctions = await store.GetInterruptedFunctions([]);
        interruptedFunctions.Count.ShouldBe(0);
    }

    public abstract Task GetInterruptedFunctionsReturnsEmptyListWhenNoneFunctionsAreInterrupted();
    protected async Task GetInterruptedFunctionsReturnsEmptyListWhenNoneFunctionsAreInterrupted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId1 = TestStoredId.Create();
        var functionId2 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());

        var session = await store.CreateFunction(
            functionId1,
            "humanInstanceId1",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        session = await store.CreateFunction(
            functionId2,
            "humanInstanceId2",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        // Don't interrupt any functions
        var interruptedFunctions = await store.GetInterruptedFunctions([functionId1, functionId2]);

        interruptedFunctions.Count.ShouldBe(0);
    }

    public abstract Task GetInterruptedFunctionsReturnsEmptyListWhenQueriedIdsDoNotExist();
    protected async Task GetInterruptedFunctionsReturnsEmptyListWhenQueriedIdsDoNotExist(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId1 = TestStoredId.Create();
        var functionId2 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());
        var nonExistentId1 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());
        var nonExistentId2 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());

        var session = await store.CreateFunction(
            functionId1,
            "humanInstanceId1",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        session = await store.CreateFunction(
            functionId2,
            "humanInstanceId2",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.Interrupt(functionId1).ShouldBeTrueAsync();
        await store.Interrupt(functionId2).ShouldBeTrueAsync();

        // Query for non-existent IDs
        var interruptedFunctions = await store.GetInterruptedFunctions([nonExistentId1, nonExistentId2]);

        interruptedFunctions.Count.ShouldBe(0);
    }

    public abstract Task GetInterruptedFunctionsOnlyReturnsMatchingInterruptedFunctions();
    protected async Task GetInterruptedFunctionsOnlyReturnsMatchingInterruptedFunctions(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId1 = TestStoredId.Create();
        var functionId2 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());
        var functionId3 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());
        var functionId4 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());

        // Create 4 functions
        var session = await store.CreateFunction(
            functionId1,
            "humanInstanceId1",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        session = await store.CreateFunction(
            functionId2,
            "humanInstanceId2",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        session = await store.CreateFunction(
            functionId3,
            "humanInstanceId3",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        session = await store.CreateFunction(
            functionId4,
            "humanInstanceId4",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        // Interrupt all 4 functions
        await store.Interrupt(functionId1).ShouldBeTrueAsync();
        await store.Interrupt(functionId2).ShouldBeTrueAsync();
        await store.Interrupt(functionId3).ShouldBeTrueAsync();
        await store.Interrupt(functionId4).ShouldBeTrueAsync();

        // Query for only functions 2 and 4
        var interruptedFunctions = await store.GetInterruptedFunctions([functionId2, functionId4]);

        // Should return only the 2 queried interrupted functions
        interruptedFunctions.Count.ShouldBe(2);
        interruptedFunctions.Any(id => id == functionId2).ShouldBeTrue();
        interruptedFunctions.Any(id => id == functionId4).ShouldBeTrue();
        interruptedFunctions.Any(id => id == functionId1).ShouldBeFalse();
        interruptedFunctions.Any(id => id == functionId3).ShouldBeFalse();
    }

    public abstract Task GetResultsReturnsResultsForExistingFunctions();
    protected async Task GetResultsReturnsResultsForExistingFunctions(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId1 = TestStoredId.Create();
        var functionId2 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());
        var functionId3 = StoredId.Create(functionId1.Type, Guid.NewGuid().ToString());

        var result1 = "result1".ToJson().ToUtf8Bytes();
        var result2 = "result2".ToJson().ToUtf8Bytes();

        // Create function 1 and succeed with result1
        await store.CreateFunction(
            functionId1,
            "humanInstanceId1",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.SucceedFunction(
            functionId1,
            result: result1,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();

        // Create function 2 and succeed with result2
        await store.CreateFunction(
            functionId2,
            "humanInstanceId2",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.SucceedFunction(
            functionId2,
            result: result2,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();

        // Create function 3 with no result (just created, not completed)
        var session = await store.CreateFunction(
            functionId3,
            "humanInstanceId3",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        // Get results for all three functions
        var results = await store.GetResults([functionId1, functionId2, functionId3]);

        // Verify results
        results.Count.ShouldBe(3);
        results[functionId1].ShouldBe(result1);
        results[functionId2].ShouldBe(result2);
        results[functionId3].ShouldBeNull();
    }

    public abstract Task GetResultsReturnsEmptyDictionaryForEmptyInput();
    protected async Task GetResultsReturnsEmptyDictionaryForEmptyInput(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;

        var results = await store.GetResults([]);

        results.ShouldNotBeNull();
        results.Count.ShouldBe(0);
    }

    public abstract Task GetResultsReturnsOnlyExistingFunctionResults();
    protected async Task GetResultsReturnsOnlyExistingFunctionResults(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var existingFunctionId = TestStoredId.Create();
        var nonExistentFunctionId = StoredId.Create(existingFunctionId.Type, Guid.NewGuid().ToString());

        var result = "my result".ToJson().ToUtf8Bytes();

        // Create and succeed one function
        await store.CreateFunction(
            existingFunctionId,
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        ).ShouldNotBeNullAsync();

        await store.SucceedFunction(
            existingFunctionId,
            result: result,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            effects: null,
            messages: null,
            storageSession: null
        ).ShouldBeTrueAsync();

        // Query for both existing and non-existent function
        var results = await store.GetResults([existingFunctionId, nonExistentFunctionId]);

        // Should only return the existing function
        results.Count.ShouldBe(1);
        results.ContainsKey(existingFunctionId).ShouldBeTrue();
        results[existingFunctionId].ShouldBe(result);
        results.ContainsKey(nonExistentFunctionId).ShouldBeFalse();
    }

    public abstract Task SetResultSucceedsWhenOwnerMatches();
    protected async Task SetResultSucceedsWhenOwnerMatches(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        var owner = ReplicaId.NewId();
        var result = "test result".ToJson().ToUtf8Bytes();

        // Create function with owner
        await store.CreateFunction(
            functionId,
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: owner
        ).ShouldNotBeNullAsync();

        // Set result
        await store.SetResult(functionId, result, owner);

        // Verify result was set
        var results = await store.GetResults([functionId]);
        results[functionId].ShouldBe(result);
    }

    public abstract Task SetResultDoesNothingWhenOwnerDoesNotMatch();
    protected async Task SetResultDoesNothingWhenOwnerDoesNotMatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        var owner = ReplicaId.NewId();
        var wrongOwner = ReplicaId.NewId();
        var result = "test result".ToJson().ToUtf8Bytes();

        // Create function with owner
        await store.CreateFunction(
            functionId,
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: owner
        ).ShouldNotBeNullAsync();

        // Try to set result with wrong owner
        await store.SetResult(functionId, result, wrongOwner);

        // Verify result was not set
        var results = await store.GetResults([functionId]);
        results[functionId].ShouldBeNull();
    }

    public abstract Task SetResultDoesNothingWhenFunctionDoesNotExist();
    protected async Task SetResultDoesNothingWhenFunctionDoesNotExist(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var nonExistentFunctionId = TestStoredId.Create();
        var owner = ReplicaId.NewId();
        var result = "test result".ToJson().ToUtf8Bytes();

        // Try to set result for non-existent function (should not throw)
        await store.SetResult(nonExistentFunctionId, result, owner);
    }
}