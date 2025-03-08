using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
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
            parent: null
        ).ShouldBeTrueAsync();
        
        var nonCompletes = await store.GetExpiredFunctions(expiresBefore: DateTime.UtcNow.Ticks);
            
        nonCompletes.Count.ShouldBe(1);
        var nonCompleted = nonCompletes[0];
        nonCompleted.FlowId.Type.ShouldBe(functionId.Type);
        nonCompleted.FlowId.Instance.ShouldBe(functionId.Instance);
        nonCompleted.Epoch.ShouldBe(0);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.StoredId.ShouldBe(functionId);
        storedFunction.Parameter.ShouldBe(paramJson.ToUtf8Bytes());
        storedFunction.Epoch.ShouldBe(0);
        storedFunction.Expires.ShouldBe(leaseExpiration);
        storedFunction.Timestamp.ShouldBe(timestamp);

        const string result = "hello world";
        var resultJson = result.ToJson();
        await store.SucceedFunction(
            functionId,
            result: resultJson.ToUtf8Bytes(),
            expectedEpoch: 0,
            timestamp: DateTime.UtcNow.Ticks,
            complimentaryState: new ComplimentaryState(() => storedParameter.ToUtf8Bytes(), LeaseLength: 0)
        ).ShouldBeTrueAsync();
            
        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Result.ShouldNotBeNull();
        storedFunction.Result.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe(result);
    }
    
    public abstract Task NullParamScenarioTest();
    protected async Task NullParamScenarioTest(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration + 1;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: null,
            leaseExpiration,
            postponeUntil: null,
            timestamp,
            parent: null
        ).ShouldBeTrueAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Parameter.ShouldBeNull();
    }

    public abstract Task LeaseIsUpdatedWhenAsExpected();
    protected async Task LeaseIsUpdatedWhenAsExpected(Task<IFunctionStore> storeTask)
    {
        var storedId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        await store.CreateFunction(
            storedId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        var affectedRows = await store.RenewLeases([new LeaseUpdate(storedId, ExpectedEpoch: 0)], leaseExpiration: 1);
        affectedRows.ShouldBe(1);

        var sf = await store.GetFunction(storedId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        sf.Expires.ShouldBe(1);
    }

    public abstract Task LeaseIsNotUpdatedWhenNotAsExpected();
    protected async Task LeaseIsNotUpdatedWhenNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.RenewLeases([new LeaseUpdate(functionId, ExpectedEpoch: 1)], leaseExpiration: 1).ShouldBeAsync(0);

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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeFalseAsync();
    }
    
    public abstract Task FunctionCreatedWithSendResultToReturnsSendResultToInStoredFunction();
    protected async Task FunctionCreatedWithSendResultToReturnsSendResultToInStoredFunction(Task<IFunctionStore> storeTask)
    {
        var functionId = TestStoredId.Create();
        var sendResultToFunctionId = TestFlowId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();

        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            paramJson.ToUtf8Bytes(), 
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

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
            parent: null
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            timestamp: DateTime.UtcNow.Ticks,
            ignoreInterrupted: false,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToUtf8Bytes().ToFunc(), LeaseLength: 0)
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
            parent: null
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            timestamp: DateTime.UtcNow.Ticks,
            ignoreInterrupted: false,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToUtf8Bytes().ToFunc(), LeaseLength: 0)
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
            parent: null
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: nowTicks,
            timestamp: DateTime.UtcNow.Ticks,
            ignoreInterrupted: false,
            expectedEpoch: 1,
            complimentaryState: new ComplimentaryState(storedParameter.ToUtf8Bytes().ToFunc(), LeaseLength: 0)
        ).ShouldBeFalseAsync();

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
        sf.Status.ShouldBe(Status.Executing);
        DefaultSerializer.Instance
            .Deserialize<string>(sf.Parameter!)
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
        var functionId = TestStoredId.Create();
        var leaseExpiration = DateTime.UtcNow.Ticks;
        
        await store.CreateFunction(
            functionId,
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
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
        var function1Id = TestStoredId.Create();
        var function2Id = function1Id with { Instance = Guid.NewGuid().ToString("N").ToStoredInstance() };

        await store.CreateFunction(
            function1Id, 
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        );
        
        await store.CreateFunction(
            function2Id, 
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: 2,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
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
        var functionId = TestStoredId.Create();

        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
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
        var functionId = TestStoredId.Create();

        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
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
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
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
            parent: null
        ).ShouldBeTrueAsync();

        var storedException = new StoredException(
            ExceptionMessage: "Something went wrong",
            ExceptionStackTrace: "StackTrace",
            ExceptionType: typeof(Exception).SimpleQualifiedName()
        );
        
        await store.FailFunction(
            storedId,
            storedException,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToUtf8Bytes().ToFunc(), LeaseLength: 0)
        );
        
        await BusyWait.Until(() => store.GetFunction(storedId).SelectAsync(sf => sf != null));

        var sf = await store.GetFunction(storedId);
        sf.ShouldNotBeNull();
        sf.Epoch.ShouldBe(0);
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
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter,
            "completed".ToJson().ToUtf8Bytes(),
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
        var functionId = TestStoredId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        var message1 = new StoredMessage(
            "hello everyone".ToJson().ToUtf8Bytes(),
            MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes(),
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
            parent: null
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToUtf8Bytes().ToFunc(), LeaseLength: 0)
        ).ShouldBeAsync(true);

        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        (sf.Epoch is 0).ShouldBeTrue();
        sf.Status.ShouldBe(Status.Suspended);
        sf.Parameter.ShouldBe(storedParameter.ToUtf8Bytes());

        var messages = await store.MessageStore.GetMessages(functionId, skip: 0);
        messages.ShouldBeEmpty();

        await Task.Delay(500);

        var functionStatus = await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );
        functionStatus.ShouldNotBeNull();
        functionStatus.Status.ShouldBe(Status.Suspended);
    }
    
    public abstract Task FunctionStatusForNonExistingFunctionIsNull();
    public async Task FunctionStatusForNonExistingFunctionIsNull(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        
        var functionStatus = await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );
        functionStatus.ShouldBeNull();
    }
    
    public abstract Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch();
    public async Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(Task<IFunctionStore> storeTask)
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
            parent: null
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
    
    public abstract Task RestartingFunctionShouldSetInterruptedToFalse();
    public async Task RestartingFunctionShouldSetInterruptedToFalse(Task<IFunctionStore> storeTask)
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
            parent: null
        ).ShouldBeTrueAsync();

        await store.Interrupt(functionId, onlyIfExecuting: false).ShouldBeTrueAsync();
        await store.Interrupted(functionId).ShouldBeAsync(true);

        await store.RestartExecution(
            functionId, 
            expectedEpoch: 0, 
            leaseExpiration: DateTime.UtcNow.Ticks
        ).ShouldNotBeNullAsync();
       
        await store.Interrupted(functionId).ShouldBeAsync(false);
    }
    
    public abstract Task MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated();
    public async Task MessagesCanBeFetchedAfterFunctionWithInitialMessagesHasBeenCreated(Task<IFunctionStore> storeTask)
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
            parent: null
        ).ShouldBeTrueAsync();

        await store.MessageStore.AppendMessage(functionId, new StoredMessage("Hello".ToJson().ToUtf8Bytes(), MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes()));
        await store.MessageStore.AppendMessage(functionId, new StoredMessage("World".ToJson().ToUtf8Bytes(), MessageType: typeof(string).SimpleQualifiedName().ToUtf8Bytes()));
        
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
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Succeeded,
            storedParameter.ToUtf8Bytes(),
            "completed".ToJson().ToUtf8Bytes(),
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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.SucceedFunction(
            functionId,
            result: null,
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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            ignoreInterrupted: false,
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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.FailFunction(
            functionId,
            new StoredException("ExceptionMessage", ExceptionStackTrace: null, typeof(Exception).SimpleQualifiedName()),
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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("some message".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );

        await store.Interrupt(functionId, onlyIfExecuting: false);
        
        await store.SuspendFunction(
            functionId,
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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.MessageStore.AppendMessage(
            functionId,
            new StoredMessage("hello world".ToJson().ToUtf8Bytes(), typeof(string).SimpleQualifiedName().ToUtf8Bytes())
        );

        await store.Interrupt(functionId, onlyIfExecuting: false);
        
        var success = await store.SuspendFunction(
            functionId,
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
        var functionId = TestStoredId.Create();
        
        var store = await storeTask;
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Interrupted.ShouldBeFalse();
        
        await store.Interrupt(functionId, onlyIfExecuting: true);
        
        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Interrupted.ShouldBeTrue();
        storedFunction.Status.ShouldBe(Status.Executing);
    }
    
    public abstract Task InterruptCountCannotBeIncrementedForNonExecutingFunction();
    protected async Task InterruptCountCannotBeIncrementedForNonExecutingFunction(Task<IFunctionStore> storeTask)
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
            parent: null
        ).ShouldBeTrueAsync();

        await store.SuspendFunction(
            functionId,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            new ComplimentaryState(() => Test.SimpleStoredParameter, LeaseLength: 0)
        ).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Interrupted.ShouldBeFalse();
        storedFunction.Status.ShouldBe(Status.Suspended);
        
        await store.Interrupt(functionId, onlyIfExecuting: true).ShouldBeFalseAsync();

        storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.Interrupted.ShouldBeFalse();
        storedFunction.Status.ShouldBe(Status.Suspended);

        var interrupted = await store.Interrupted(functionId);
        interrupted.HasValue.ShouldBeTrue();
        interrupted!.Value.ShouldBeFalse();
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
        await store.CreateFunction(
            functionId, 
            "humanInstanceId",
            param: Test.SimpleStoredParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await effectsStore.SetEffectResult(functionId, new StoredEffect(EffectId: "".ToEffectId(EffectType.State), "".ToStoredEffectId(EffectType.State), WorkStatus.Completed, "some default state".ToUtf8Bytes(), StoredException: null));

        var storedEffects = await effectsStore.GetEffectResults(functionId);
        storedEffects.Count.ShouldBe(1);
        storedEffects.Single().EffectId.ShouldBe("".ToEffectId(EffectType.State));
        storedEffects.Single().Result.ShouldBe("some default state".ToUtf8Bytes());
    }
    
    public abstract Task SucceededFunctionsCanBeFetchedSuccessfully();
    protected async Task SucceededFunctionsCanBeFetchedSuccessfully(Task<IFunctionStore> storeTask)
    {
        var functionId1 = TestStoredId.Create();
        var functionId2 = functionId1 with { Instance = Guid.NewGuid().ToString().ToStoredInstance() };
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
                parent: null
            ).ShouldBeTrueAsync();

            await store.SucceedFunction(
                functionId,
                result: null,
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
        var parent = TestStoredId.Create();
        var typeId = TestStoredId.Create().Type;
        var functionIds = Enumerable
            .Range(0, 500)
            .Select(i => new StoredId(typeId, Instance: i.ToString().ToStoredInstance()))
            .ToList();
        
        await store.BulkScheduleFunctions(
            functionIds.Select(functionId => new IdWithParam(functionId, "humanInstanceId", Param: functionId.ToString().ToUtf8Bytes())),
            parent
        );

        var eligibleFunctions = 
            await store.GetExpiredFunctions(DateTime.UtcNow.Ticks);
        
        eligibleFunctions.Count.ShouldBe(functionIds.Count);
        foreach (var flowId in functionIds)
        {
            eligibleFunctions.Any(f => f.FlowId == flowId).ShouldBeTrue();
        }

        foreach (var id in functionIds)
        {
            var sf = await store.GetFunction(id);
            sf.ShouldNotBeNull();
            sf.Parameter!.ToStringFromUtf8Bytes().ShouldBe(id.ToString());
            sf.ParentId.ShouldBe(parent);
        }
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
        await store.CreateFunction(
            flowId1, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: 0,
            timestamp,
            parent: null
        ).ShouldBeTrueAsync();
        await store.CreateFunction(
            flowId2, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: 0,
            timestamp,
            parent: null
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
        var store = await storeTask;

        var storedType1 = await store.TypeStore.InsertOrGetStoredType("FlowType1");
        var flowId1 = TestStoredId.Create() with { Type = storedType1 };
        var flowId2 = TestStoredId.Create() with { Type = storedType1 };
        
        var storedType2 = await store.TypeStore.InsertOrGetStoredType("FlowType2");
        var flowId3 = TestStoredId.Create() with { Type = storedType2 };

        var leaseExpiration = DateTime.UtcNow.Ticks;
        var timestamp = leaseExpiration;
        
        await store.CreateFunction(
            flowId1, 
            "humanInstanceId",
            Test.SimpleStoredParameter,
            leaseExpiration,
            postponeUntil: 0,
            timestamp,
            parent: null
        ).ShouldBeTrueAsync();
        await store.CreateFunction(
            flowId2, 
            "humanInstanceId",
            Test.SimpleStoredParameter,
            leaseExpiration,
            postponeUntil: 0,
            timestamp,
            parent: null
        ).ShouldBeTrueAsync();
        await store.CreateFunction(
            flowId3, 
            "humanInstanceId",
            Test.SimpleStoredParameter,
            leaseExpiration,
            postponeUntil: 0,
            timestamp,
            parent: null
        ).ShouldBeTrueAsync();
        
        var instances = await store.GetInstances(storedType1);
        instances.Count.ShouldBe(2);
        instances.Any(i => i == flowId1.Instance).ShouldBeTrue();
        instances.Any(i => i == flowId2.Instance).ShouldBeTrue();

        await store.SucceedFunction(
            flowId1,
            result: null,
            timestamp,
            expectedEpoch: 0,
            new ComplimentaryState(StoredParameterFunc: () => default, LeaseLength: 0)
        );

        instances = await store.GetInstances(storedType1, Status.Succeeded);
        instances.Count.ShouldBe(1);
        instances.Single().ShouldBe(flowId1.Instance);

        var flowTypes = await store.TypeStore.GetAllFlowTypes();
        flowTypes.Count.ShouldBe(2);
        flowTypes.Values.Any(t => t == flowId1.Type).ShouldBeTrue();
        flowTypes.Values.Any(t => t == flowId3.Type).ShouldBeTrue();
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
        await store.CreateFunction(
            id,
            humanInstanceId: "SomeInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parentId
        ).ShouldBeTrueAsync();

        var sf = await store.GetFunction(id).ShouldNotBeNullAsync();
        sf.ParentId.ShouldBe(parentId);
    }
    
    public abstract Task FlowWithWithoutParentIsReturnsNullParentInSubsequentGetTest();
    protected async Task FlowWithWithoutParentIsReturnsNullParentInSubsequentGetTest(Task<IFunctionStore> storeTask)
    {
        var id = TestStoredId.Create();
        var store = await storeTask;
        await store.CreateFunction(
            id,
            humanInstanceId: "SomeInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null
        ).ShouldBeTrueAsync();

        var sf = await store.GetFunction(id).ShouldNotBeNullAsync();
        sf.ParentId.ShouldBeNull();
    }
    
    public abstract Task BatchOfLeasesCanBeUpdatedSimultaneously();
    protected async Task BatchOfLeasesCanBeUpdatedSimultaneously(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        
        var id1 = TestStoredId.Create();
        var id2 = TestStoredId.Create();
        var id3 = TestStoredId.Create();
        
        await store.CreateFunction(
            id1,
            humanInstanceId: "SomeInstanceId",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null
        ).ShouldBeTrueAsync();
        await store.CreateFunction(
            id2,
            humanInstanceId: "SomeInstanceId2",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null
        ).ShouldBeTrueAsync();
        await store.RestartExecution(id2, expectedEpoch: 0, leaseExpiration: 0);
        await store.CreateFunction(
            id3,
            humanInstanceId: "SomeInstanceId3",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null
        ).ShouldBeTrueAsync();

        await store.RenewLeases(
            leaseUpdates: [
                new LeaseUpdate(id1, ExpectedEpoch: 0),
                new LeaseUpdate(id2, ExpectedEpoch: 1)
            ],
            leaseExpiration: 10_000
        );
        
        var sf1 = await store.GetFunction(id1).ShouldNotBeNullAsync();
        var sf2 = await store.GetFunction(id2).ShouldNotBeNullAsync();
        
    }
    
    public abstract Task MultipleFunctionsStatusCanBeFetched();
    protected async Task MultipleFunctionsStatusCanBeFetched(Task<IFunctionStore> storeTask)
    {
        var functionId1 = TestStoredId.Create();
        var functionId2 = functionId1 with { Instance = Guid.NewGuid().ToString().ToStoredInstance() };
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
                parent: null
            ).ShouldBeTrueAsync();

            await store.SucceedFunction(
                functionId,
                result: null,
                timestamp: timestamp,
                expectedEpoch: 0,
                new ComplimentaryState(() => Test.SimpleStoredParameter, LeaseLength: 0)
            ).ShouldBeTrueAsync();
        }

        await CreateAndSucceedFunction(functionId1, timestamp: 1);
        await CreateAndSucceedFunction(functionId2, timestamp: 3);
        await CreateAndSucceedFunction(functionId3, timestamp: 0);

        var statuses = await store.GetFunctionsStatus([functionId1, functionId3]);
        var statusAndEpoch1 = statuses.Single(s => s.StoredId == functionId1);
        statusAndEpoch1.Epoch.ShouldBe(0);
        statusAndEpoch1.Status.ShouldBe(Status.Succeeded);
        
        var statusAndEpoch2 = statuses.Single(s => s.StoredId == functionId1);
        statusAndEpoch2.Epoch.ShouldBe(0);
        statusAndEpoch2.Status.ShouldBe(Status.Succeeded);
    }
    
    public abstract Task InterruptedFunctionIsNotPostponedWhenFlagIsSet();
    protected async Task InterruptedFunctionIsNotPostponedWhenFlagIsSet(Task<IFunctionStore> storeTask)
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
            parent: null
        ).ShouldBeTrueAsync();

        await store.Interrupt([storedId]);

        var success = await store.PostponeFunction(
            storedId,
            postponeUntil: 0,
            timestamp: 0,
            ignoreInterrupted: false,
            expectedEpoch: 0,
            new ComplimentaryState(() => Test.SimpleStoredParameter, LeaseLength: 0)
        );
        success.ShouldBeFalse();

        var sf = await store.GetFunction(storedId).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Executing);
        sf.Epoch.ShouldBe(0);
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
            parent: null
        ).ShouldBeTrueAsync();

        await store.Interrupt([storedId]);

        var success = await store.PostponeFunction(
            storedId,
            postponeUntil: 0,
            timestamp: 0,
            ignoreInterrupted: true,
            expectedEpoch: 0,
            new ComplimentaryState(() => Test.SimpleStoredParameter, LeaseLength: 0)
        );
        success.ShouldBeTrue();

        var sf = await store.GetFunction(storedId).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Postponed);
    }
}