using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StoreCrudTests
{
    private StoredId StoredId { get; } = TestStoredId.Create();
    private TestParameters TestParam { get; } = new TestParameters("Peter", 32);
    private string Param => TestParam.ToJson();
    private record TestParameters(string Name, int Age);

    private class TestFlowState : FlowState
    {
        public string? Note { get; set; }
    }
        
    public abstract Task FunctionCanBeCreatedWithASingleParameterSuccessfully();
    protected async Task FunctionCanBeCreatedWithASingleParameterSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.Initialize();
        
        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(StoredId);
        stored!.StoredId.ShouldBe(StoredId);
        stored.Parameter.ShouldBe(Param.ToUtf8Bytes());
        stored.Result.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.Epoch.ShouldBe(0);
        stored.Expires.ShouldBe(leaseExpiration);
    }

    public abstract Task FunctionCanBeCreatedWithTwoParametersSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(StoredId);
        stored!.StoredId.ShouldBe(StoredId);
        stored.Parameter.ShouldBe(Param.ToUtf8Bytes());
        stored.Result.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.Epoch.ShouldBe(0);
        stored.Expires.ShouldBe(leaseExpiration);
    }
        
    public abstract Task FunctionCanBeCreatedWithTwoParametersAndStateSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersAndStateSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(StoredId);
        stored!.StoredId.ShouldBe(StoredId);
        stored.Parameter.ShouldBe(Param.ToUtf8Bytes());
        stored.Result.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.Epoch.ShouldBe(0);
        stored.Expires.ShouldBe(leaseExpiration);
    }

    public abstract Task FetchingNonExistingFunctionReturnsNull();
    protected async Task FetchingNonExistingFunctionReturnsNull(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.GetFunction(StoredId).ShouldBeNullAsync();
    }  
   
    public abstract Task LeaseIsUpdatedWhenCurrentEpochMatches();
    protected async Task LeaseIsUpdatedWhenCurrentEpochMatches(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.RenewLease(StoredId, expectedEpoch: 0, leaseExpiration: 1).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(StoredId);
        storedFunction!.Epoch.ShouldBe(0);
        storedFunction.Expires.ShouldBe(1);
    }
    
    public abstract Task LeaseIsNotUpdatedWhenCurrentEpochIsDifferent();
    protected async Task LeaseIsNotUpdatedWhenCurrentEpochIsDifferent(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.RenewLease(StoredId, expectedEpoch: 1, leaseExpiration: 1).ShouldBeFalseAsync();

        var storedFunction = await store.GetFunction(StoredId);
        storedFunction!.Epoch.ShouldBe(0);
        storedFunction.Expires.ShouldBe(leaseExpiration);
    }

    public abstract Task ExistingFunctionCanBeDeleted();
    public async Task ExistingFunctionCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestStoredId.Create();
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.EffectsStore.SetEffectResult(
            functionId,
            StoredEffect.CreateState(new StoredState("SomeStateId", "SomeStateJson".ToUtf8Bytes()))
        );
        await store.CorrelationStore.SetCorrelation(functionId, "SomeCorrelationId");
        await store.EffectsStore.SetEffectResult(
            functionId,
            new StoredEffect("SomeEffectId".ToEffectId(), "SomeEffectId".ToStoredEffectId(EffectType.Effect), WorkStatus.Completed, Result: null, StoredException: null)
        );
        await store.MessageStore.AppendMessage(functionId, new StoredMessage("SomeJson".ToUtf8Bytes(), "SomeType".ToUtf8Bytes()));
        await store.TimeoutStore.UpsertTimeout(
            new StoredTimeout(functionId, "SomeTimeoutId", Expiry: DateTime.UtcNow.AddDays(1).Ticks),
            overwrite: false
        );
        
        await store.DeleteFunction(functionId);

        await store.GetFunction(functionId).ShouldBeNullAsync();
        await store.CorrelationStore.GetCorrelations(functionId).ShouldBeEmptyAsync();
        await store.EffectsStore.GetEffectResults(functionId).ShouldBeEmptyAsync();
        await store.MessageStore.GetMessages(functionId, skip: 0).ShouldBeEmptyAsync();
        await store.TimeoutStore.GetTimeouts(functionId).ShouldBeEmptyAsync();
    }
    
    public abstract Task NonExistingFunctionCanBeDeleted();
    public async Task NonExistingFunctionCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.DeleteFunction(StoredId);
    }

    public abstract Task ParameterAndStateCanBeUpdatedOnExistingFunction();
    public async Task ParameterAndStateCanBeUpdatedOnExistingFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        var updatedStoredParameter = "hello world".ToJson();

        await store.SetParameters(
            StoredId,
            updatedStoredParameter.ToUtf8Bytes(),
            result: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
        
        var sf = await store.GetFunction(StoredId);
        sf.ShouldNotBeNull();
        sf.Parameter.ShouldNotBeNull();
        var param = sf.Parameter.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>();
        param.ShouldBe("hello world");
    }
    
    public abstract Task ParameterCanBeUpdatedOnExistingFunction();
    public async Task ParameterCanBeUpdatedOnExistingFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        var updatedStoredParameter = "hello world".ToJson();

        await store.SetParameters(
            StoredId,
            updatedStoredParameter.ToUtf8Bytes(),
            result: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
        
        var sf = await store.GetFunction(StoredId);
        sf.ShouldNotBeNull();
        var param = sf.Parameter!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>();
        param.ShouldBe("hello world");
    }
    
    public abstract Task StateCanBeUpdatedOnExistingFunction();
    public async Task StateCanBeUpdatedOnExistingFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();
        
        await store.SetParameters(
            StoredId,
            param: Param.ToUtf8Bytes(),
            result: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
        
        var sf = await store.GetFunction(StoredId);
        sf.ShouldNotBeNull();
        sf.Parameter.ShouldNotBeNull();
    }
    
    public abstract Task ParameterAndStateAreNotUpdatedWhenEpochDoesNotMatch();
    public async Task ParameterAndStateAreNotUpdatedWhenEpochDoesNotMatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();
        await store.RestartExecution(
            StoredId,
            expectedEpoch: 0,
            leaseExpiration: DateTime.UtcNow.Ticks
        ).ShouldNotBeNullAsync();

        var updatedStoredParameter = "hello world".ToJson();

        await store.SetParameters(
            StoredId,
            updatedStoredParameter.ToUtf8Bytes(),
            result: null,
            expectedEpoch: 0
        ).ShouldBeFalseAsync();
        
        var sf = await store.GetFunction(StoredId);
        sf.ShouldNotBeNull();
        sf.Parameter.ShouldNotBeNull();
    }
}