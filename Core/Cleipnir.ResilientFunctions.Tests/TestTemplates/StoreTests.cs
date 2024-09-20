using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StoreTests
{
    private const string PARAM = "param";

    public abstract Task SunshineScenarioTest();
    protected async Task SunshineScenarioTest(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var storedParameter = paramJson;

        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration + 1;
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration,
            postponeUntil: null,
            timestamp
        ).ShouldBeTrueAsync();
        
        var nonCompletes = await store.GetExpiredFunctions(expiresBefore: DateTime.UtcNow.Ticks);
            
        nonCompletes.Count.ShouldBe(1);
        var nonCompleted = nonCompletes[0];
        nonCompleted.FlowId.Type.ShouldBe(functionId.Type);
        nonCompleted.FlowId.Instance.ShouldBe(functionId.Instance);
        nonCompleted.Epoch.ShouldBe(0);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.FlowId.ShouldBe(functionId);
        storedFunction.Parameter.ShouldBe(paramJson);
        storedFunction.Epoch.ShouldBe(0);
        storedFunction.Expires.ShouldBe(leaseExpiration);
        storedFunction.Timestamp.ShouldBe(timestamp);
        storedFunction.DefaultState.ShouldBeNull();

        const string result = "hello world";
        var resultJson = result.ToJson();
        await store.SucceedFunction(
            functionId,
            result: resultJson,
            defaultState: null,
            expectedEpoch: 0,
            timestamp: DateTime.UtcNow.Ticks,
            complimentaryState: new ComplimentaryState(() => storedParameter, LeaseLength: 0)
        ).ShouldBeTrueAsync();
            
        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Result.ShouldNotBeNull();
        storedFunction.Result.DeserializeFromJsonTo<string>().ShouldBe(result);
    }
    
    public abstract Task NullParamScenarioTest();
    protected async Task NullParamScenarioTest(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration + 1;
        await store.CreateFunction(
            functionId,
            param: null,
            leaseExpiration,
            postponeUntil: null,
            timestamp
        ).ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Parameter.ShouldBeNull();
    }

    public abstract Task LeaseIsUpdatedWhenAsExpected();
    protected async Task LeaseIsUpdatedWhenAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        await store.CreateFunction(
            functionId,
            paramJson,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store
            .RenewLease(functionId, expectedEpoch: 0, leaseExpiration: 1)
            .ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        sf.Expires.ShouldBe(1);
    }

    public abstract Task LeaseIsNotUpdatedWhenNotAsExpected();
    protected async Task LeaseIsNotUpdatedWhenNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            functionId,
            paramJson, 
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RenewLease(
            functionId,  
            expectedEpoch: 1,
            leaseExpiration: 1
        ).ShouldBeFalseAsync();

        await store
            .GetExpiredFunctions(expiresBefore: leaseExpiration + 1)
            .ShouldBeNonEmptyAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        
        sf.Epoch.ShouldBe(0);
        sf.Expires.ShouldBe(leaseExpiration);
    }
        
    public abstract Task BecomeLeaderSucceedsWhenEpochIsAsExpected();
    protected async Task BecomeLeaderSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        await store.CreateFunction(
            functionId,
            paramJson, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store
            .RestartExecution(
                functionId,
                expectedEpoch: 0,
                leaseExpiration
            ).ShouldNotBeNullAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(1);
        storedFunction.Expires.ShouldBe(leaseExpiration);
    }
        
    public abstract Task BecomeLeaderFailsWhenEpochIsNotAsExpected();
    protected async Task BecomeLeaderFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            functionId,
            paramJson, 
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
        
        await store
            .RestartExecution(
                functionId,
                expectedEpoch: 1,
                leaseExpiration: DateTime.UtcNow.Ticks
            ).ShouldBeNullAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
        storedFunction.Expires.ShouldBe(leaseExpiration);
    }

    public abstract Task CreatingTheSameFunctionTwiceReturnsFalse();
    protected async Task CreatingTheSameFunctionTwiceReturnsFalse(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        await store.CreateFunction(
            functionId,
            paramJson, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.CreateFunction(
            functionId,
            paramJson, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeFalseAsync();
    }
    
    public abstract Task FunctionCreatedWithSendResultToReturnsSendResultToInStoredFunction();
    protected async Task FunctionCreatedWithSendResultToReturnsSendResultToInStoredFunction(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        var sendResultToFunctionId = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        await store.CreateFunction(
            functionId,
            paramJson, 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
    }
    
    public abstract Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut();
    protected async Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();

        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var nowTicks = DateTime.UtcNow.Ticks;

        var storedParameter = paramJson;
        
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToFunc(), LeaseLength: 0)
        ).ShouldBeTrueAsync();
        
        var postponedFunctions = await store.GetExpiredFunctions(expiresBefore: nowTicks - 100);
        postponedFunctions.ShouldBeEmpty();
    }
    
    public abstract Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut();
    protected async Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();

        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var nowTicks = DateTime.UtcNow.Ticks;

        var storedParameter = paramJson;
        
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToFunc(), LeaseLength: 0)
        ).ShouldBeTrueAsync();
        
        var postponedFunctions = await store.GetExpiredFunctions(expiresBefore: nowTicks + 100);
        postponedFunctions.Count().ShouldBe(1);
    }
    
    public abstract Task PostponeFunctionFailsWhenEpochIsNotAsExpected();
    protected async Task PostponeFunctionFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();

        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();
        var nowTicks = DateTime.UtcNow.Ticks;

        var storedParameter = paramJson;
         
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 1,
            complimentaryState: new ComplimentaryState(storedParameter.ToFunc(), LeaseLength: 0)
        ).ShouldBeFalseAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        sf.Status.ShouldBe(Status.Executing);
        DefaultSerializer.Instance
            .DeserializeParameter<string>(sf.Parameter!)
            .ShouldBe(PARAM);
    }
    
    public abstract Task InitializeCanBeInvokedMultipleTimesSuccessfully();
    protected async Task InitializeCanBeInvokedMultipleTimesSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.Initialize();
        await store.Initialize();
    }
    
    public abstract Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency();
    protected async Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var leaseExpiration = DateTime.UtcNow.Ticks;
        
        await store.CreateFunction(
            functionId,
            "hello world".ToJson(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        
        var storedFunctions = await store.GetExpiredFunctions(expiresBefore: DateTime.UtcNow.Ticks);
        storedFunctions.Count.ShouldBe(1);

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Expires.ShouldBe(leaseExpiration);
    }
    
    public abstract Task OnlyEligibleCrashedFunctionsAreReturnedFromStore();
    protected async Task OnlyEligibleCrashedFunctionsAreReturnedFromStore(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var function1Id = TestFlowId.Create();
        var function2Id = new FlowId(function1Id.Type, flowInstance: Guid.NewGuid().ToString("N"));

        await store.CreateFunction(
            function1Id,
            "hello world".ToJson(),
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        
        await store.CreateFunction(
            function2Id,
            "hello world".ToJson(),
            leaseExpiration: 2,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        
        var storedFunctions = await store.GetExpiredFunctions(expiresBefore: 1);
        storedFunctions.Count.ShouldBe(1);
        var (flowId, epoch) = storedFunctions[0];
        flowId.ShouldBe(function1Id);
        epoch.ShouldBe(0);
    }
    
    public abstract Task IncrementEpochSucceedsWhenEpochIsAsExpected();
    protected async Task IncrementEpochSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();

        await store.CreateFunction(
            functionId,
            "hello world".ToJson(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RestartExecution(functionId, expectedEpoch: 0, DateTime.UtcNow.Ticks).ShouldNotBeNullAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(1);
    }
    
    public abstract Task IncrementEpochFailsWhenEpochIsNotAsExpected();
    protected async Task IncrementEpochFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();

        await store.CreateFunction(
            functionId,
            "hello world".ToJson(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RestartExecution(functionId, expectedEpoch: 1, leaseExpiration: DateTime.UtcNow.Ticks).ShouldBeNullAsync();
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
    }
    
    private class FlowState : Domain.FlowState
    {
        public string State { get; set; } = "";
    }
    
    public abstract Task DeletingExistingFunctionSucceeds();
    public async Task DeletingExistingFunctionSucceeds(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

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
        var functionId = TestFlowId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var storedException = new StoredException(
            ExceptionMessage: "Something went wrong",
            ExceptionStackTrace: "StackTrace",
            ExceptionType: typeof(Exception).SimpleQualifiedName()
        );
        
        await store.FailFunction(
            functionId,
            storedException,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToFunc(), LeaseLength: 0)
        );
        
        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        sf.Status.ShouldBe(Status.Failed);
        sf.Exception.ShouldNotBeNull();
        var previouslyThrownException = DefaultSerializer.Instance.DeserializeException(sf.Exception);
        previouslyThrownException.ErrorMessage.ShouldBe(storedException.ExceptionMessage);
        previouslyThrownException.StackTrace.ShouldBe(storedException.ExceptionStackTrace);
        previouslyThrownException.ErrorType.ShouldBe(typeof(Exception));
    }
    
    public abstract Task SetFunctionStateSucceedsWhenEpochIsAsExpected();
    public async Task SetFunctionStateSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            "completed".ToJson(),
            storedException: null,
            expires: DateTime.UtcNow.Ticks,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(1);
        sf.Status.ShouldBe(Status.Succeeded);
        sf.Exception.ShouldBeNull();
    }
    
    public abstract Task SetFunctionStateSucceedsWithMessagesWhenEpochIsAsExpected();
    public async Task SetFunctionStateSucceedsWithMessagesWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var messages = store.MessageStore;
        var functionId = TestFlowId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var message1 = new StoredMessage(
            "hello everyone".ToJson(),
            MessageType: typeof(string).SimpleQualifiedName(),
            IdempotencyKey: "idempotency_key_1"
        );
        await messages.AppendMessage(functionId, message1);

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            "completed".ToJson(),
            storedException: null,
            expires: DateTime.Now.Ticks,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(1);
        sf.Status.ShouldBe(Status.Succeeded);
        sf.Exception.ShouldBeNull();

        var storedMessages = await store.MessageStore.GetMessages(functionId, skip: 0);
        storedMessages.Count.ShouldBe(1);
        var deserializedMessage = (string) DefaultSerializer.Instance.DeserializeMessage(storedMessages[0].MessageJson, storedMessages[0].MessageType);
        deserializedMessage.ShouldBe("hello everyone");
    }
    
    public abstract Task ExecutingFunctionCanBeSuspendedSuccessfully();
    public async Task ExecutingFunctionCanBeSuspendedSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
            expectedInterruptCount: 0,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToFunc(), LeaseLength: 0)
        ).ShouldBeAsync(true);

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        (sf.Epoch is 0).ShouldBeTrue();
        sf.Status.ShouldBe(Status.Suspended);
        sf.Parameter.ShouldBe(storedParameter);

        var messages = await store.MessageStore.GetMessages(functionId, skip: 0);
        messages.ShouldBeEmpty();

        await Task.Delay(500);

        var functionStatus = await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson(), MessageType: typeof(string).SimpleQualifiedName())
        );
        functionStatus.ShouldNotBeNull();
        functionStatus.Status.ShouldBe(Status.Suspended);
    }
    
    public abstract Task FunctionStatusForNonExistingFunctionIsNull();
    public async Task FunctionStatusForNonExistingFunctionIsNull(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        
        var functionStatus = await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson(), MessageType: typeof(string).SimpleQualifiedName())
        );
        functionStatus.ShouldBeNull();
    }
    
    public abstract Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch();
    public async Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RestartExecution(
            functionId, 
            expectedEpoch: 0, 
            leaseExpiration: DateTime.UtcNow.Ticks
        ).ShouldNotBeNullAsync();
        await store.RestartExecution(
            functionId, 
            expectedEpoch: 0, 
            leaseExpiration: DateTime.UtcNow.Ticks
        ).ShouldBeNullAsync();
    }
    
    public abstract Task MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated();
    public async Task MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.MessageStore.AppendMessage(functionId, new StoredMessage("Hello".ToJson(), MessageType: typeof(string).SimpleQualifiedName()));
        await store.MessageStore.AppendMessage(functionId, new StoredMessage("World".ToJson(), MessageType: typeof(string).SimpleQualifiedName()));
        
        var messages = await store.MessageStore.GetMessages(functionId, skip: 0);
        messages.Count.ShouldBe(2);
        messages[0].DefaultDeserialize().ShouldBe("Hello");
        messages[1].DefaultDeserialize().ShouldBe("World");
    }
    
    public abstract Task FunctionStatusAndEpochCanBeSuccessfullyFetched();
    public async Task FunctionStatusAndEpochCanBeSuccessfullyFetched(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            "completed".ToJson(),
            storedException: null,
            expires: DateTime.Now.Ticks,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));

        var (status, epoch) = await store.GetFunctionStatus(functionId).ShouldNotBeNullAsync();
        status.ShouldBe(Status.Succeeded);
        epoch.ShouldBe(1);
    }
    
    public abstract Task EpochIsNotIncrementedOnCompletion();
    protected async Task EpochIsNotIncrementedOnCompletion(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SucceedFunction(
            functionId,
            result: null,
            defaultState: null,
            DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(Test.SimpleStoredParameter.ToFunc(), LeaseLength: 0)
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
    }
    
    public abstract Task EpochIsNotIncrementedOnPostponed();
    protected async Task EpochIsNotIncrementedOnPostponed(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.Ticks,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(Test.SimpleStoredParameter.ToFunc(), LeaseLength: 0)
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
    }
    
    public abstract Task EpochIsNotIncrementedOnFailure();
    protected async Task EpochIsNotIncrementedOnFailure(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.FailFunction(
            functionId,
            new StoredException("ExceptionMessage", ExceptionStackTrace: null, typeof(Exception).SimpleQualifiedName()),
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(Test.SimpleStoredParameter.ToFunc(), LeaseLength: 0)
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Epoch.ShouldBe(0);
    }
    
    public abstract Task EpochIsNotIncrementedOnSuspension();
    protected async Task EpochIsNotIncrementedOnSuspension(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
            expectedInterruptCount: 0,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(Test.SimpleStoredParameter.ToFunc(), LeaseLength: 0)
        );
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        (storedFunction.Epoch is 0).ShouldBeTrue();
    }
    
    public abstract Task SuspensionDoesNotSucceedOnExpectedMessagesCountMismatchButPostponesFunction();
    protected async Task SuspensionDoesNotSucceedOnExpectedMessagesCountMismatchButPostponesFunction(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("some message".ToJson(), typeof(string).SimpleQualifiedName())
        );

        await store.IncrementInterruptCount(functionId).ShouldBeTrueAsync();
        
        await store.SuspendFunction(
            functionId,
            expectedInterruptCount: 0,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(Test.SimpleStoredParameter.ToFunc(), LeaseLength: 0)
        ).ShouldBeFalseAsync();
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        (storedFunction.Status is Status.Executing).ShouldBeTrue();
    }
    
    public abstract Task FunctionIsStillExecutingOnSuspensionAndInterruptCountMismatch();
    protected async Task FunctionIsStillExecutingOnSuspensionAndInterruptCountMismatch(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson(), typeof(string).SimpleQualifiedName())
        );

        await store.IncrementInterruptCount(functionId).ShouldBeTrueAsync();
        
        var success = await store.SuspendFunction(
            functionId,
            expectedInterruptCount: 0,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(Test.SimpleStoredParameter.ToFunc(), LeaseLength: 0)
        );
        
        success.ShouldBeFalse();
        
        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        (storedFunction.Epoch is 0).ShouldBeTrue();
        storedFunction.Status.ShouldBe(Status.Executing);
    }
    
    public abstract Task InterruptCountCanBeIncrementedForExecutingFunction();
    protected async Task InterruptCountCanBeIncrementedForExecutingFunction(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.InterruptCount.ShouldBe(0);
        
        await store.IncrementInterruptCount(functionId);
        
        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.InterruptCount.ShouldBe(1);
        storedFunction.Status.ShouldBe(Status.Executing);

        await store.GetInterruptCount(functionId).ShouldBeAsync(1);
    }
    
    public abstract Task InterruptCountCannotBeIncrementedForNonExecutingFunction();
    protected async Task InterruptCountCannotBeIncrementedForNonExecutingFunction(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
            expectedInterruptCount: 0,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            new ComplimentaryState(() => Test.SimpleStoredParameter, LeaseLength: 0)
        ).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.InterruptCount.ShouldBe(0);
        storedFunction.Status.ShouldBe(Status.Suspended);
        
        await store.IncrementInterruptCount(functionId).ShouldBeFalseAsync();
        
        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.InterruptCount.ShouldBe(0);
        storedFunction.Status.ShouldBe(Status.Suspended);

        await store.GetInterruptCount(functionId).ShouldBeAsync(0);
    }
    
    public abstract Task InterruptCountForNonExistingFunctionIsNull();
    protected async Task InterruptCountForNonExistingFunctionIsNull(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        var store = await storeTask;
        (await store.GetInterruptCount(functionId)).ShouldBeNull();
    }
    
    public abstract Task DefaultStateCanSetAndFetchedAfterwards();
    protected async Task DefaultStateCanSetAndFetchedAfterwards(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SetDefaultState(functionId, "some default state");

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.DefaultState.ShouldBe("some default state");
    }
    
    public abstract Task DefaultStateCanSetOnPostponeAndFetchedAfterwards();
    protected async Task DefaultStateCanSetOnPostponeAndFetchedAfterwards(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: 0,
            defaultState: "some default state",
            timestamp: 0,
            expectedEpoch: 0,
            new ComplimentaryState(() => Test.SimpleStoredParameter, LeaseLength: 0)
        ).ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.DefaultState.ShouldBe("some default state");
    }
    
    public abstract Task DefaultStateCanSetOnSuspendAndFetchedAfterwards();
    protected async Task DefaultStateCanSetOnSuspendAndFetchedAfterwards(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
            expectedInterruptCount: 0,
            defaultState: "some default state",
            timestamp: 0,
            expectedEpoch: 0,
            new ComplimentaryState(() => Test.SimpleStoredParameter, LeaseLength: 0)
        ).ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.DefaultState.ShouldBe("some default state");
    }
    
    public abstract Task DefaultStateCanSetOnSucceedAndFetchedAfterwards();
    protected async Task DefaultStateCanSetOnSucceedAndFetchedAfterwards(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId,
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.SucceedFunction(
            functionId,
            result: null,
            defaultState: "some default state",
            timestamp: 0,
            expectedEpoch: 0,
            new ComplimentaryState(() => Test.SimpleStoredParameter, LeaseLength: 0)
        ).ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.DefaultState.ShouldBe("some default state");
    }
    
    public abstract Task SucceededFunctionsCanBeFetchedSuccessfully();
    protected async Task SucceededFunctionsCanBeFetchedSuccessfully(Task<IFunctionStore> storeTask)
    {
        var functionId1 = TestFlowId.Create();
        var functionId2 = TestFlowId.Create().WithTypeId(functionId1.Type);
        var functionId3 = TestFlowId.Create();
        var store = await storeTask;

        async Task CreateAndSucceedFunction(FlowId functionId, long timestamp)
        {
            await store.CreateFunction(
                functionId,
                param: Test.SimpleStoredParameter,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: timestamp
            ).ShouldBeTrueAsync();

            await store.SucceedFunction(
                functionId,
                result: null,
                defaultState: null,
                timestamp: timestamp,
                expectedEpoch: 0,
                new ComplimentaryState(() => Test.SimpleStoredParameter, LeaseLength: 0)
            ).ShouldBeTrueAsync();
        }

        await CreateAndSucceedFunction(functionId1, timestamp: 1);
        await CreateAndSucceedFunction(functionId2, timestamp: 3);
        await CreateAndSucceedFunction(functionId3, timestamp: 0);

        var succeededFunctions = await store.GetSucceededFunctions(functionId1.Type, completedBefore: 2);
        succeededFunctions.Count.ShouldBe(1);
        succeededFunctions.Single().ShouldBe(functionId1.Instance);
    }
    
    public abstract Task BulkScheduleInsertsAllFunctionsSuccessfully();
    protected async Task BulkScheduleInsertsAllFunctionsSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        
        var typeId = TestFlowId.Create().Type;
        var functionIds = Enumerable
            .Range(0, 101)
            .Select(_ => TestFlowId.Create().WithTypeId(typeId))
            .ToList();
        
        await store.BulkScheduleFunctions(
            functionIds.Select(functionId => new IdWithParam(functionId, Param: ""))
        );

        var eligibleFunctions = 
            await store.GetExpiredFunctions(DateTime.UtcNow.Ticks);
        
        eligibleFunctions.Count.ShouldBe(functionIds.Count);
        foreach (var flowId in functionIds)
        {
            eligibleFunctions.Any(f => f.FlowId == flowId).ShouldBeTrue();
        }
    }
    
    public abstract Task DifferentTypesAreFetchedByGetExpiredFunctionsCall();
    protected async Task DifferentTypesAreFetchedByGetExpiredFunctionsCall(Task<IFunctionStore> storeTask)
    {
        var flowId1 = TestFlowId.Create();
        var flowId2 = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var storedParameter = paramJson;

        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration + 1;
        await store.CreateFunction(
            flowId1,
            storedParameter,
            leaseExpiration,
            postponeUntil: 0,
            timestamp
        ).ShouldBeTrueAsync();
        await store.CreateFunction(
            flowId2,
            storedParameter,
            leaseExpiration,
            postponeUntil: 0,
            timestamp
        ).ShouldBeTrueAsync();
        
        var expires = await store.GetExpiredFunctions(expiresBefore: DateTime.UtcNow.Ticks);
        
        expires.Count.ShouldBe(2);
        var flowIds = expires.Select(x => x.FlowId).ToHashSet();
        flowIds.Contains(flowId1).ShouldBeTrue();
        flowIds.Contains(flowId2).ShouldBeTrue();
    }
    
    public abstract Task MultipleInstancesCanBeFetchedForFlowType();
    protected async Task MultipleInstancesCanBeFetchedForFlowType(Task<IFunctionStore> storeTask)
    {
        var flowType = TestFlowId.Create().Type; 
        var flowId1 = TestFlowId.Create().WithTypeId(flowType);
        var flowId2 = TestFlowId.Create().WithTypeId(flowType);
        
        var flowId3 = TestFlowId.Create();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration;
        
        var store = await storeTask;
        
        await store.CreateFunction(
            flowId1,
            Test.SimpleStoredParameter,
            leaseExpiration,
            postponeUntil: 0,
            timestamp
        ).ShouldBeTrueAsync();
        await store.CreateFunction(
            flowId2,
            Test.SimpleStoredParameter,
            leaseExpiration,
            postponeUntil: 0,
            timestamp
        ).ShouldBeTrueAsync();
        await store.CreateFunction(
            flowId3,
            Test.SimpleStoredParameter,
            leaseExpiration,
            postponeUntil: 0,
            timestamp
        ).ShouldBeTrueAsync();
        
        var instances = await store.GetInstances(flowType);
        instances.Count.ShouldBe(2);
        instances.Any(i => i == flowId1.Instance).ShouldBeTrue();
        instances.Any(i => i == flowId2.Instance).ShouldBeTrue();

        await store.SucceedFunction(
            flowId1,
            result: null,
            defaultState: null,
            timestamp,
            expectedEpoch: 0,
            new ComplimentaryState(StoredParameterFunc: () => default, LeaseLength: 0)
        );

        instances = await store.GetInstances(flowType, Status.Succeeded);
        instances.Count.ShouldBe(1);
        instances.Single().ShouldBe(flowId1.Instance);
    }
}