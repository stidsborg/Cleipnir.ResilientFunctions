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
        var functionId = TestFunctionId.Create();
        
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

        await BusyWait.Until(() => 
            store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks).SelectAsync(efs => efs.Any())
        );
        
        var nonCompletes = await store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks).ToListAsync();
            
        nonCompletes.Count.ShouldBe(1);
        var nonCompleted = nonCompletes[0];
        nonCompleted.InstanceId.ShouldBe(functionId.InstanceId);
        nonCompleted.Epoch.ShouldBe(0);
        nonCompleted.LeaseExpiration.ShouldBe(leaseExpiration);

        var storedFunction = await store.GetFunction(functionId);
        storedFunction.ShouldNotBeNull();
        storedFunction.FunctionId.ShouldBe(functionId);
        storedFunction.Parameter.ShouldBe(paramJson);
        storedFunction.Epoch.ShouldBe(0);
        storedFunction.LeaseExpiration.ShouldBe(leaseExpiration);
        storedFunction.Timestamp.ShouldBe(timestamp);
        storedFunction.PostponedUntil.ShouldBeNull();
        storedFunction.DefaultState.ShouldBeNull();

        const string result = "hello world";
        var resultJson = result.ToJson();
        var resultType = result.GetType().SimpleQualifiedName();
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

    public abstract Task LeaseIsUpdatedWhenAsExpected();
    protected async Task LeaseIsUpdatedWhenAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

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

        await BusyWait.Until(() =>
            store
                .GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks)
                .SelectAsync(efs => efs.Any())
        );

        await BusyWait.Until(async () =>
        {
            var nonCompletedFunctions = await store
                .GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks)
                .ToListAsync();
            if (!nonCompletedFunctions.Any()) return false;
            
            var nonCompletedFunction = nonCompletedFunctions.Single();
            return nonCompletedFunction is { Epoch: 0, LeaseExpiration: 1 };
        });
    }

    public abstract Task LeaseIsNotUpdatedWhenNotAsExpected();
    protected async Task LeaseIsNotUpdatedWhenNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
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

        await BusyWait.Until(() =>
            store
                .GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks)
                .SelectAsync(efs => efs.Any())
        );
        
        var nonCompletedFunctions = 
            await store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks);
        
        var nonCompletedFunction = nonCompletedFunctions.Single();
        nonCompletedFunction.Epoch.ShouldBe(0);
        nonCompletedFunction.LeaseExpiration.ShouldBe(leaseExpiration);
    }
        
    public abstract Task BecomeLeaderSucceedsWhenEpochIsAsExpected();
    protected async Task BecomeLeaderSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
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
        storedFunction.LeaseExpiration.ShouldBe(leaseExpiration);
    }
        
    public abstract Task BecomeLeaderFailsWhenEpochIsNotAsExpected();
    protected async Task BecomeLeaderFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
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
        storedFunction.LeaseExpiration.ShouldBe(leaseExpiration);
    }

    public abstract Task CreatingTheSameFunctionTwiceReturnsFalse();
    protected async Task CreatingTheSameFunctionTwiceReturnsFalse(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

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
        var functionId = TestFunctionId.Create();
        var sendResultToFunctionId = TestFunctionId.Create();
        
        var store = await storeTask;
        var paramJson = PARAM.ToJson();
        var paramType = PARAM.GetType().SimpleQualifiedName();

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
        var functionId = TestFunctionId.Create();

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

        await BusyWait.Until(() => store
            .GetPostponedFunctions(functionId.TypeId, isEligibleBefore: nowTicks + 100)
            .SelectAsync(pfs => pfs.Any())
        );
        
        var postponedFunctions = await store.GetPostponedFunctions(
            functionId.TypeId,
            isEligibleBefore: nowTicks - 100
        );
        postponedFunctions.ShouldBeEmpty();
    }
    
    public abstract Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut();
    protected async Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();

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

        await BusyWait.Until(() => store
            .GetPostponedFunctions(functionId.TypeId, nowTicks + 100)
            .SelectAsync(pfs => pfs.Any())
        );
        
        var postponedFunctions = await store.GetPostponedFunctions(
            functionId.TypeId,
            isEligibleBefore: nowTicks + 100
        );
        postponedFunctions.Count().ShouldBe(1);
    }
    
    public abstract Task PostponeFunctionFailsWhenEpochIsNotAsExpected();
    protected async Task PostponeFunctionFailsWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();

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
        var functionId = TestFunctionId.Create();
        var leaseExpiration = DateTime.UtcNow.Ticks;
        
        await store.CreateFunction(
            functionId,
            "hello world".ToJson(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );

        await BusyWait.Until(() => store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks).Any());
        
        var storedFunctions = await store.GetCrashedFunctions(functionId.TypeId, leaseExpiresBefore: DateTime.UtcNow.Ticks).ToListAsync();
        storedFunctions.Count.ShouldBe(1);
        var sf = storedFunctions[0];
        sf.LeaseExpiration.ShouldBe(leaseExpiration);
    }
    
    public abstract Task OnlyEligibleCrashedFunctionsAreReturnedFromStore();
    protected async Task OnlyEligibleCrashedFunctionsAreReturnedFromStore(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var function1Id = TestFunctionId.Create();
        var function2Id = new FunctionId(function1Id.TypeId, functionInstanceId: Guid.NewGuid().ToString("N"));

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

        await BusyWait.Until(() => store.GetCrashedFunctions(function1Id.TypeId, leaseExpiresBefore: 1).Any());
        
        var storedFunctions = await store.GetCrashedFunctions(function1Id.TypeId, leaseExpiresBefore: 1).ToListAsync();
        storedFunctions.Count.ShouldBe(1);
        var sf = storedFunctions[0];
        sf.InstanceId.ShouldBe(function1Id.InstanceId);
        sf.LeaseExpiration.ShouldBe(0);
    }
    
    public abstract Task IncrementEpochSucceedsWhenEpochIsAsExpected();
    protected async Task IncrementEpochSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

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
        var functionId = TestFunctionId.Create();

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
    
    private class WorkflowState : Domain.WorkflowState
    {
        public string State { get; set; } = "";
    }
    
    public abstract Task DeletingExistingFunctionSucceeds();
    public async Task DeletingExistingFunctionSucceeds(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

        var storedParameter = "hello world".ToJson();
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));
        
        await store.DeleteFunction(functionId);
        
        var sf = await store.GetFunction(functionId);
        sf.ShouldBeNull();
    }

    public abstract Task FailFunctionSucceedsWhenEpochIsAsExpected();
    public async Task FailFunctionSucceedsWhenEpochIsAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

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
        var functionId = TestFunctionId.Create();

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
            postponeUntil: null,
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
        var functionId = TestFunctionId.Create();

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
            postponeUntil: null,
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
        var functionId = TestFunctionId.Create();

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
        functionStatus.Status.ShouldBe(Status.Suspended);
    }
    
    public abstract Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch();
    public async Task RestartingExecutionShouldFailWhenExpectedEpochDoesNotMatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();

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
        var functionId = TestFunctionId.Create();

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
        var functionId = TestFunctionId.Create();

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
            postponeUntil: null,
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        var store = await storeTask;
        (await store.GetInterruptCount(functionId)).ShouldBeNull();
    }
    
    public abstract Task DefaultStateCanSetAndFetchedAfterwards();
    protected async Task DefaultStateCanSetAndFetchedAfterwards(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
        var functionId = TestFunctionId.Create();
        
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
}