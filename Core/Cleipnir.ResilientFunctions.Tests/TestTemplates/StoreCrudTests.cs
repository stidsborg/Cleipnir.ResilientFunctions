using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
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
        
        var session = await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
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
        stored.Expires.ShouldBe(0);
    }

    public abstract Task FunctionCanBeCreatedWithTwoParametersSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var session = await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
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
        stored.Expires.ShouldBe(0);
    }

    public abstract Task FunctionCanBeCreatedWithTwoParametersAndStateSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersAndStateSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var session = await store.CreateFunction(
            StoredId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
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
        stored.Expires.ShouldBe(0);
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
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: owner
        ).ShouldNotBeNullAsync();

        var owners = await store.GetOwnerReplicas();
        owners.Single().ShouldBe(owner);
        await store.RescheduleCrashedFunctions(owner);

        var newOwner = Guid.NewGuid().ToReplicaId();
        await store.ClaimFunction(StoredId, newOwner);
        
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
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner
        ).ShouldNotBeNullAsync();

        var owners = await store.GetOwnerReplicas();
        owners.Single().ShouldBe(owner);

        var newOwner = Guid.NewGuid().ToReplicaId();
        await store.ClaimFunction(StoredId, newOwner);
        
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
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        session.ShouldBeNull();

        await store.EffectsStore.SetEffectResult(
            storedId,
            StoredEffect.CreateCompleted(1.ToEffectId(), "SomeStateJson".ToUtf8Bytes(), alias: null).ToStoredChange(storedId, Insert),
            session: null
        );
        await store.EffectsStore.SetEffectResult(
            storedId,
            new StoredEffect(2.ToEffectId(), WorkStatus.Completed, Result: null, StoredException: null, Alias: null).ToStoredChange(storedId, Insert),
            session: null
        );
        await store.MessageStore.AppendMessage(storedId, new StoredMessage("SomeJson".ToUtf8Bytes(), "SomeType".ToUtf8Bytes(), Replica: ReplicaId.Empty, Position: 0));

        await store.DeleteFunction(storedId);

        await store.GetFunction(storedId).ShouldBeNullAsync();
        await store.EffectsStore.GetEffectResults(storedId).ShouldBeEmptyAsync();
        await store.MessageStore.GetMessages(storedId).ShouldBeEmptyAsync();
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

    public abstract Task ClaimFunctionsReturnsEmptyDictionaryWhenNoFlowsAreEligible();
    public async Task ClaimFunctionsReturnsEmptyDictionaryWhenNoFlowsAreEligible(Task<IFunctionStore> storeTask)
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
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: existingOwner
        );
        await store.CreateFunction(
            storedId2,
            "instance2",
            Param.ToUtf8Bytes(),
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: existingOwner
        );

        // Try to restart - should return empty dictionary
        var result = await store.ClaimFunctions([storedId1, storedId2], newOwner);

        result.Count.ShouldBe(0);
    }

    public abstract Task ClaimFunctionsRestartsMultipleUnownedFlows();
    public async Task ClaimFunctionsRestartsMultipleUnownedFlows(Task<IFunctionStore> storeTask)
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
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        await store.CreateFunction(
            storedId2,
            "instance2",
            Param.ToUtf8Bytes(),
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        await store.CreateFunction(
            storedId3,
            "instance3",
            Param.ToUtf8Bytes(),
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );

        // Restart all three
        var result = await store.ClaimFunctions([storedId1, storedId2, storedId3], owner);

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

    public abstract Task ClaimFunctionsRestartsOnlyUnownedFlows();
    public async Task ClaimFunctionsRestartsOnlyUnownedFlows(Task<IFunctionStore> storeTask)
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
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        await store.CreateFunction(
            storedId2,
            "instance2",
            Param.ToUtf8Bytes(),
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: existingOwner
        );
        await store.CreateFunction(
            storedId3,
            "instance3",
            Param.ToUtf8Bytes(),
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );

        // Restart all three - only 1 and 3 should succeed
        var result = await store.ClaimFunctions([storedId1, storedId2, storedId3], newOwner);

        result.Count.ShouldBe(2);
        result.ContainsKey(storedId1).ShouldBeTrue();
        result.ContainsKey(storedId2).ShouldBeFalse();
        result.ContainsKey(storedId3).ShouldBeTrue();

        result[storedId1].StoredFlow.OwnerId.ShouldBe(newOwner);
        result[storedId3].StoredFlow.OwnerId.ShouldBe(newOwner);
    }

    public abstract Task ClaimFunctionsReturnsEmptyDictionaryForEmptyInput();
    public async Task ClaimFunctionsReturnsEmptyDictionaryForEmptyInput(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var owner = ReplicaId.NewId();

        var result = await store.ClaimFunctions([], owner);

        result.Count.ShouldBe(0);
    }

    public abstract Task ClaimFunctionsIncludesExistingEffects();
    public async Task ClaimFunctionsIncludesExistingEffects(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId1 = TestStoredId.Create();
        var storedId2 = TestStoredId.Create();
        var owner = ReplicaId.NewId();

        // Create effects
        var effect1 = new StoredEffect(
            EffectId: "effect1".GetHashCode().ToEffectId(),
            WorkStatus: WorkStatus.Completed,
            Result: "result1".ToUtf8Bytes(),
            StoredException: null,
            Alias: null
        );
        var effect2 = new StoredEffect(
            EffectId: "effect2".GetHashCode().ToEffectId(),
            WorkStatus: WorkStatus.Completed,
            Result: "result2".ToUtf8Bytes(),
            StoredException: null,
            Alias: null
        );

        // Create messages - these must NOT be fetched by ClaimFunctions
        var message1 = new StoredMessage(
            MessageContent: "message1".ToUtf8Bytes(),
            MessageType: "Type1".ToUtf8Bytes(),
            Position: 0,
            Replica: ReplicaId.Empty,
            IdempotencyKey: null
        );
        var message2 = new StoredMessage(
            MessageContent: "message2".ToUtf8Bytes(),
            MessageType: "Type2".ToUtf8Bytes(),
            Position: 1,
            Replica: ReplicaId.Empty,
            IdempotencyKey: null
        );

        // Create 2 functions with effects and messages
        await store.CreateFunction(
            storedId1,
            "instance1",
            Param.ToUtf8Bytes(),
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null,
            effects: [effect1],
            messages: [message1]
        );
        await store.CreateFunction(
            storedId2,
            "instance2",
            Param.ToUtf8Bytes(),
            postponeUntil: DateTime.UtcNow.Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null,
            effects: [effect2],
            messages: [message2]
        );

        // Restart both
        var result = await store.ClaimFunctions([storedId1, storedId2], owner);

        // Verify both flows returned with their effects
        result.Count.ShouldBe(2);

        result.ContainsKey(storedId1).ShouldBeTrue();
        result.ContainsKey(storedId2).ShouldBeTrue();

        // Verify flow 1 has correct effects
        var flow1 = result[storedId1];
        flow1.Effects.Count.ShouldBe(1);
        flow1.Effects[0].EffectId.ShouldBe("effect1".GetHashCode().ToEffectId());
        flow1.Effects[0].Result.ShouldBe("result1".ToUtf8Bytes());

        // Verify flow 2 has correct effects
        var flow2 = result[storedId2];
        flow2.Effects.Count.ShouldBe(1);
        flow2.Effects[0].EffectId.ShouldBe("effect2".GetHashCode().ToEffectId());
        flow2.Effects[0].Result.ShouldBe("result2".ToUtf8Bytes());
    }

    public abstract Task SetFunctionUpdatesStatusOwnerAndEffectsWhenGuardMatches();
    protected async Task SetFunctionUpdatesStatusOwnerAndEffectsWhenGuardMatches(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var owner = ReplicaId.NewId();
        var newOwner = ReplicaId.NewId();

        await store.CreateFunction(
            storedId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: owner
        ).ShouldNotBeNullAsync();

        var effect = StoredEffect.CreateCompleted(1.ToEffectId(), "SomeResult".ToUtf8Bytes(), alias: null);

        var success = await store.SetFunction(
            storedId,
            Status.Postponed,
            param: "UpdatedParam".ToUtf8Bytes(),
            result: "TheResult".ToUtf8Bytes(),
            exception: null,
            expires: 123,
            timestamp: 456,
            owner: newOwner,
            effects: [effect],
            expectedReplica: owner,
            storageSession: null
        );
        success.ShouldBeTrue();

        var sf = await store.GetFunction(storedId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Postponed);
        sf.OwnerId.ShouldBe(newOwner);
        sf.Expires.ShouldBe(123);
        sf.Timestamp.ShouldBe(456);
        sf.Parameter.ShouldBe("UpdatedParam".ToUtf8Bytes());

        var results = await store.GetResults([storedId]);
        results[storedId].ShouldBe("TheResult".ToUtf8Bytes());

        var effects = await store.EffectsStore.GetEffectResults(storedId);
        effects.Count.ShouldBe(1);
        effects.Single().EffectId.ShouldBe(1.ToEffectId());
        effects.Single().Result.ShouldBe("SomeResult".ToUtf8Bytes());
    }

    public abstract Task SetFunctionReturnsFalseAndNoOpsWhenExpectedReplicaDoesNotMatch();
    protected async Task SetFunctionReturnsFalseAndNoOpsWhenExpectedReplicaDoesNotMatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var owner = ReplicaId.NewId();
        var wrongReplica = ReplicaId.NewId();
        var newOwner = ReplicaId.NewId();

        await store.CreateFunction(
            storedId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            postponeUntil: null,
            timestamp: 100,
            parent: null,
            owner: owner
        ).ShouldNotBeNullAsync();

        var effect = StoredEffect.CreateCompleted(1.ToEffectId(), "SomeResult".ToUtf8Bytes(), alias: null);

        var success = await store.SetFunction(
            storedId,
            Status.Succeeded,
            param: "UpdatedParam".ToUtf8Bytes(),
            result: "TheResult".ToUtf8Bytes(),
            exception: null,
            expires: 123,
            timestamp: 456,
            owner: newOwner,
            effects: [effect],
            expectedReplica: wrongReplica,
            storageSession: null
        );
        success.ShouldBeFalse();

        // Nothing was updated - the flow is left exactly as it was created.
        var sf = await store.GetFunction(storedId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Executing);
        sf.OwnerId.ShouldBe(owner);
        sf.Parameter.ShouldBe(Param.ToUtf8Bytes());

        var effects = await store.EffectsStore.GetEffectResults(storedId);
        effects.ShouldBeEmpty();
    }

    public abstract Task SetFunctionWithNullOwnerReleasesOwnership();
    protected async Task SetFunctionWithNullOwnerReleasesOwnership(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var owner = ReplicaId.NewId();

        await store.CreateFunction(
            storedId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: owner
        ).ShouldNotBeNullAsync();

        var success = await store.SetFunction(
            storedId,
            Status.Succeeded,
            param: Param.ToUtf8Bytes(),
            result: "TheResult".ToUtf8Bytes(),
            exception: null,
            expires: 0,
            timestamp: 789,
            owner: null,
            effects: null,
            expectedReplica: owner,
            storageSession: null
        );
        success.ShouldBeTrue();

        var sf = await store.GetFunction(storedId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Succeeded);
        sf.OwnerId.ShouldBeNull();

        (await store.GetOwnerReplicas()).ShouldNotContain(owner);
    }

    public abstract Task SetFunctionWithNullEffectsLeavesEffectsUntouched();
    protected async Task SetFunctionWithNullEffectsLeavesEffectsUntouched(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var owner = ReplicaId.NewId();

        var existingEffect = StoredEffect.CreateCompleted(7.ToEffectId(), "ExistingResult".ToUtf8Bytes(), alias: null);
        await store.CreateFunction(
            storedId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: owner,
            effects: [existingEffect]
        ).ShouldNotBeNullAsync();

        var success = await store.SetFunction(
            storedId,
            Status.Postponed,
            param: Param.ToUtf8Bytes(),
            result: null,
            exception: null,
            expires: 10,
            timestamp: 20,
            owner: owner,
            effects: null,
            expectedReplica: owner,
            storageSession: null
        );
        success.ShouldBeTrue();

        var sf = await store.GetFunction(storedId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Postponed);

        // effects: null must leave the effect snapshot untouched.
        var effects = await store.EffectsStore.GetEffectResults(storedId);
        effects.Count.ShouldBe(1);
        effects.Single().EffectId.ShouldBe(7.ToEffectId());
        effects.Single().Result.ShouldBe("ExistingResult".ToUtf8Bytes());
    }

    public abstract Task SetFunctionKeepsPassedSessionCoherentWithPersistedEffects();
    protected async Task SetFunctionKeepsPassedSessionCoherentWithPersistedEffects(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var replica = ReplicaId.NewId();

        // Create the flow unowned with an initial effect so it carries a snapshot/version, then claim it to obtain
        // the store's owned session (the exact object handed over ClaimFunction -> SetFunction).
        var initialEffect = StoredEffect.CreateCompleted(1.ToEffectId(), "initial".ToUtf8Bytes(), alias: null);
        await store.CreateFunction(
            storedId,
            "humanInstanceId",
            Param.ToUtf8Bytes(),
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null,
            effects: [initialEffect]
        );

        var claimed = await store.ClaimFunction(storedId, replica).ShouldNotBeNullAsync();
        var session = (SnapshotStorageSession) claimed.StorageSession;

        // Wholesale-replace the snapshot via SetFunction, keeping the flow owned by the claiming replica and
        // threading the claimed session through.
        var newEffectA = StoredEffect.CreateCompleted(2.ToEffectId(), "two".ToUtf8Bytes(), alias: null);
        var newEffectB = StoredEffect.CreateCompleted(3.ToEffectId(), "three".ToUtf8Bytes(), alias: null);

        var success = await store.SetFunction(
            storedId,
            Status.Executing,
            param: Param.ToUtf8Bytes(),
            result: null,
            exception: null,
            expires: 0,
            timestamp: DateTime.UtcNow.Ticks,
            owner: replica,
            effects: [newEffectA, newEffectB],
            expectedReplica: replica,
            storageSession: session
        );
        success.ShouldBeTrue();

        // (b) The threaded session now reflects the persisted snapshot exactly.
        session.RowExists.ShouldBeTrue();
        session.Effects.Count.ShouldBe(2);
        session.Effects.ContainsKey(1.ToEffectId()).ShouldBeFalse();
        session.Effects[2.ToEffectId()].Result.ShouldBe("two".ToUtf8Bytes());
        session.Effects[3.ToEffectId()].Result.ShouldBe("three".ToUtf8Bytes());

        // The persisted snapshot matches too.
        var persisted = await store.EffectsStore.GetEffectResults(storedId);
        persisted.Count.ShouldBe(2);
        persisted.Select(e => e.EffectId).ShouldBe([2.ToEffectId(), 3.ToEffectId()], ignoreOrder: true);

        // A subsequent effect write reusing the SAME session must NOT be rejected by a stale version - i.e. the
        // session stayed in lockstep with the store's effect version/owner concurrency.
        var followUpEffect = StoredEffect.CreateCompleted(4.ToEffectId(), "four".ToUtf8Bytes(), alias: null);
        await store.EffectsStore.SetEffectResult(
            storedId,
            followUpEffect.ToStoredChange(storedId, Insert),
            session
        );

        var afterFollowUp = await store.EffectsStore.GetEffectResults(storedId);
        afterFollowUp.Select(e => e.EffectId).ShouldContain(4.ToEffectId());
    }
}