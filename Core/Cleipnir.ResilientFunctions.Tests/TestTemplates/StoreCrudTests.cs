using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Storage.CrudOperation;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StoreCrudTests
{
    private StoredId StoredId { get; } = TestStoredId.Create();
    private TestParameters TestParam { get; } = new TestParameters("Peter", 32);
    private string Param => TestParam.ToJson();
    private record TestParameters(string Name, int Age);
        
    public abstract Task FunctionCanBeCreatedWithASingleParameterSuccessfully();
    protected async Task FunctionCanBeCreatedWithASingleParameterSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        
        var leaseExpiration = DateTime.UtcNow.Ticks;
        var session = await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var stored = await store.GetFunction(StoredId);
        stored!.StoredId.ShouldBe(StoredId);
        stored.Parameter.ShouldBe(Param.ToUtf8Bytes());
        var results = await store.GetResults([StoredId]);
        var resultBytes = results.TryGetValue(StoredId, out var rb) ? rb : null;
        resultBytes.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.Expires.ShouldBe(leaseExpiration);
    }

    public abstract Task FunctionCanBeCreatedWithTwoParametersSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        var session = await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var stored = await store.GetFunction(StoredId);
        stored!.StoredId.ShouldBe(StoredId);
        stored.Parameter.ShouldBe(Param.ToUtf8Bytes());
        var results = await store.GetResults([StoredId]);
        var resultBytes = results.TryGetValue(StoredId, out var rb) ? rb : null;
        resultBytes.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.Expires.ShouldBe(leaseExpiration);
    }

    public abstract Task FunctionCanBeCreatedWithTwoParametersAndStateSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersAndStateSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        var session = await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var stored = await store.GetFunction(StoredId);
        stored!.StoredId.ShouldBe(StoredId);
        stored.Parameter.ShouldBe(Param.ToUtf8Bytes());
        var results = await store.GetResults([StoredId]);
        var resultBytes = results.TryGetValue(StoredId, out var rb) ? rb : null;
        resultBytes.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
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
        var owner = Guid.NewGuid().ToReplicaId();
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: owner
        ).ShouldNotBeNullAsync();

        var owners = await store.GetOwnerReplicas();
        owners.Single().ShouldBe(owner);
        await store.RescheduleCrashedFunctions(owner);

        var newOwner = Guid.NewGuid().ToReplicaId();
        await store.RestartExecution(StoredId, newOwner);
        
        var storedFunction = await store.GetFunction(StoredId);
        storedFunction.ShouldNotBeNull();
        storedFunction.OwnerId.ShouldBe(newOwner);
    }
    
    public abstract Task LeaseIsNotUpdatedWhenCurrentEpochIsDifferent();
    protected async Task LeaseIsNotUpdatedWhenCurrentEpochIsDifferent(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var owner = Guid.NewGuid().ToReplicaId();
        await store.CreateFunction(
            StoredId, 
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner
        ).ShouldNotBeNullAsync();

        var owners = await store.GetOwnerReplicas();
        owners.Single().ShouldBe(owner);

        var newOwner = Guid.NewGuid().ToReplicaId();
        await store.RestartExecution(StoredId, newOwner);
        
        var storedFunction = await store.GetFunction(StoredId);
        storedFunction.ShouldNotBeNull();
        storedFunction.OwnerId.ShouldBe(owner);
    }

    public abstract Task ExistingFunctionCanBeDeleted();
    public async Task ExistingFunctionCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var session = await store.CreateFunction(
            storedId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.EffectsStore.SetEffectResult(
            storedId,
            StoredEffect.CreateState(new StoredState("SomeStateId", "SomeStateJson".ToUtf8Bytes())).ToStoredChange(storedId, Insert),
            session: null
        );
        await store.CorrelationStore.SetCorrelation(storedId, "SomeCorrelationId");
        await store.EffectsStore.SetEffectResult(
            storedId,
            new StoredEffect("SomeEffectId".ToEffectId(), WorkStatus.Completed, Result: null, StoredException: null).ToStoredChange(storedId, Insert),
            session: null
        );
        await store.MessageStore.AppendMessage(storedId, new StoredMessage("SomeJson".ToUtf8Bytes(), "SomeType".ToUtf8Bytes(), Position: 0));
        
        await store.DeleteFunction(storedId);

        await store.GetFunction(storedId).ShouldBeNullAsync();
        await store.CorrelationStore.GetCorrelations(storedId).ShouldBeEmptyAsync();
        await store.EffectsStore.GetEffectResults(storedId).ShouldBeEmptyAsync();
        await store.MessageStore.GetMessages(storedId, skip: 0).ShouldBeEmptyAsync();
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
        var session = await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var updatedStoredParameter = "hello world".ToJson();

        await store.SetParameters(
            StoredId,
            updatedStoredParameter.ToUtf8Bytes(),
            result: null,
            expectedReplica: null
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
        var session = await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        var updatedStoredParameter = "hello world".ToJson();

        await store.SetParameters(
            StoredId,
            updatedStoredParameter.ToUtf8Bytes(),
            result: null,
            expectedReplica: null
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
        var session = await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();
        
        await store.SetParameters(
            StoredId,
            param: Param.ToUtf8Bytes(),
            result: null,
            expectedReplica: null
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
            parent: null,
            owner: ReplicaId.NewId()
        ).ShouldNotBeNullAsync();

        var updatedStoredParameter = "hello world".ToJson();

        await store.SetParameters(
            StoredId,
            updatedStoredParameter.ToUtf8Bytes(),
            result: null,
            expectedReplica: ReplicaId.Empty
        ).ShouldBeFalseAsync();
        
        var sf = await store.GetFunction(StoredId);
        sf.ShouldNotBeNull();
        sf.Parameter.ShouldNotBeNull();
    }

    public abstract Task RestartExecutionsRestartsMultipleUnownedFlows();
    public async Task RestartExecutionsRestartsMultipleUnownedFlows(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId1 = TestStoredId.Create();
        var storedId2 = TestStoredId.Create();
        var storedId3 = TestStoredId.Create();
        var owner = ReplicaId.NewId();

        // Create 3 functions without owners (postponed)
        await store.CreateFunction(
            storedId1,
            "instance1",
            Param.ToUtf8Bytes(),
            leaseExpiration: 0,
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        await store.CreateFunction(
            storedId2,
            "instance2",
            Param.ToUtf8Bytes(),
            leaseExpiration: 0,
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        await store.CreateFunction(
            storedId3,
            "instance3",
            Param.ToUtf8Bytes(),
            leaseExpiration: 0,
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );

        // Restart all three
        var result = await store.RestartExecutions([storedId1, storedId2, storedId3], owner);

        // All three should be restarted
        result.Count.ShouldBe(3);
        result.ContainsKey(storedId1).ShouldBeTrue();
        result.ContainsKey(storedId2).ShouldBeTrue();
        result.ContainsKey(storedId3).ShouldBeTrue();

        result[storedId1].StoredFlow.OwnerId.ShouldBe(owner);
        result[storedId1].StoredFlow.Status.ShouldBe(Status.Executing);
        result[storedId2].StoredFlow.OwnerId.ShouldBe(owner);
        result[storedId2].StoredFlow.Status.ShouldBe(Status.Executing);
        result[storedId3].StoredFlow.OwnerId.ShouldBe(owner);
        result[storedId3].StoredFlow.Status.ShouldBe(Status.Executing);
    }

    public abstract Task RestartExecutionsReturnsEmptyDictionaryWhenNoFlowsAreEligible();
    public async Task RestartExecutionsReturnsEmptyDictionaryWhenNoFlowsAreEligible(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId1 = TestStoredId.Create();
        var storedId2 = TestStoredId.Create();
        var existingOwner = ReplicaId.NewId();
        var newOwner = ReplicaId.NewId();

        // Create 2 functions with existing owners
        await store.CreateFunction(
            storedId1,
            "instance1",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: existingOwner
        );
        await store.CreateFunction(
            storedId2,
            "instance2",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: existingOwner
        );

        // Try to restart - should return empty dictionary
        var result = await store.RestartExecutions([storedId1, storedId2], newOwner);

        result.Count.ShouldBe(0);
    }

    public abstract Task RestartExecutionsRestartsOnlyUnownedFlows();
    public async Task RestartExecutionsRestartsOnlyUnownedFlows(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId1 = TestStoredId.Create();
        var storedId2 = TestStoredId.Create();
        var storedId3 = TestStoredId.Create();
        var existingOwner = ReplicaId.NewId();
        var newOwner = ReplicaId.NewId();

        // Create 2 functions without owners and 1 with owner
        await store.CreateFunction(
            storedId1,
            "instance1",
            Param.ToUtf8Bytes(),
            leaseExpiration: 0,
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        await store.CreateFunction(
            storedId2,
            "instance2",
            Param.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: existingOwner
        );
        await store.CreateFunction(
            storedId3,
            "instance3",
            Param.ToUtf8Bytes(),
            leaseExpiration: 0,
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );

        // Restart all three - only 1 and 3 should succeed
        var result = await store.RestartExecutions([storedId1, storedId2, storedId3], newOwner);

        result.Count.ShouldBe(2);
        result.ContainsKey(storedId1).ShouldBeTrue();
        result.ContainsKey(storedId2).ShouldBeFalse();
        result.ContainsKey(storedId3).ShouldBeTrue();

        result[storedId1].StoredFlow.OwnerId.ShouldBe(newOwner);
        result[storedId3].StoredFlow.OwnerId.ShouldBe(newOwner);
    }

    public abstract Task RestartExecutionsReturnsEmptyDictionaryForEmptyInput();
    public async Task RestartExecutionsReturnsEmptyDictionaryForEmptyInput(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var owner = ReplicaId.NewId();

        var result = await store.RestartExecutions([], owner);

        result.Count.ShouldBe(0);
    }
}