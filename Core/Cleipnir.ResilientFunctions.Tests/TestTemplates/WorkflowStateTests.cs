using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class WorkflowStateTests
{
    private FunctionId FunctionId { get; } = new FunctionId("typeId", "instanceId");
    private StoredParameter Param { get; } = new StoredParameter(
        ParamJson: "HelloWorld".ToJson(),
        ParamType: typeof(string).SimpleQualifiedName()
    );
        
    private class WorkflowState : Domain.WorkflowState
    {
        public string? Name { get; set; }
    }
        
    public abstract Task SunshineScenario();
    protected async Task SunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredState(new WorkflowState().ToJson(), typeof(WorkflowState).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
            
        var state = new WorkflowState();
        state.Initialize(onSave: async () =>
        {
            var storedParam = DefaultSerializer.Instance.SerializeParameter(Param);
            var storedState = DefaultSerializer.Instance.SerializeState(state);
            await store.SaveStateForExecutingFunction(
                FunctionId,
                storedState.StateJson,
                expectedEpoch: 0,
                complimentaryState: new ComplimentaryState(() => storedParam, () => storedState, LeaseLength: 0)
            );
        });

        var storedState = (await store.GetFunction(FunctionId))!.State;
        storedState.ShouldNotBeNull();
        storedState.StateType.ShouldBe(typeof(WorkflowState).SimpleQualifiedName());
        storedState.StateJson.ShouldNotBeNull();

        await state.Save();

        storedState = (await store.GetFunction(FunctionId))!.State;
        storedState.ShouldNotBeNull();
        storedState.StateType.ShouldBe(typeof(WorkflowState).SimpleQualifiedName());
        storedState.StateJson.ShouldNotBeNull();
        storedState.StateJson!.DeserializeFromJsonTo<WorkflowState>()!.Name.ShouldBeNull(); 
            
        state.Name = "Peter"; 
        await state.Save();
            
        storedState = (await store.GetFunction(FunctionId))!.State;
        storedState.ShouldNotBeNull();
        storedState.StateType.ShouldBe(typeof(WorkflowState).SimpleQualifiedName());
        storedState.StateJson.ShouldNotBeNull();
        storedState.StateJson!.DeserializeFromJsonTo<WorkflowState>()!.Name.ShouldBe("Peter");
            
        state.Name = "Ole"; 
        await state.Save();
            
        storedState = (await store.GetFunction(FunctionId))!.State;
        storedState.ShouldNotBeNull();
        storedState.StateType.ShouldBe(typeof(WorkflowState).SimpleQualifiedName());
        storedState.StateJson.ShouldNotBeNull();
        storedState.StateJson!.DeserializeFromJsonTo<WorkflowState>()!.Name.ShouldBe("Ole");
    }

    public abstract Task StateIsNotUpdatedWhenEpochIsNotAsExpected();
    protected async Task StateIsNotUpdatedWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredState(new Domain.WorkflowState().ToJson(), typeof(Domain.WorkflowState).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
        await store.RestartExecution(
            FunctionId,
            expectedEpoch: 0, 
            leaseExpiration: DateTime.UtcNow.Ticks
        ).ShouldNotBeNullAsync();
        
        var state = new WorkflowState() {Name = "Peter"};
        state.Initialize(onSave: async () =>
        {
            var storedParam = DefaultSerializer.Instance.SerializeParameter(Param);
            var storedState = DefaultSerializer.Instance.SerializeState(state);
            await store.SaveStateForExecutingFunction(
                FunctionId,
                storedState.StateJson,
                expectedEpoch: 1,
                complimentaryState: new ComplimentaryState(() => storedParam, () => storedState, LeaseLength: 0)
            );
        });
        await state.Save();
            
        state = new WorkflowState() {Name = "Ole"};
        state.Initialize(onSave: async () =>
        {
            var storedParam = DefaultSerializer.Instance.SerializeParameter(Param);
            var storedState = DefaultSerializer.Instance.SerializeState(state);
            await store.SaveStateForExecutingFunction(
                FunctionId,
                storedState.StateJson,
                expectedEpoch: 0,
                complimentaryState: new ComplimentaryState(() => storedParam, () => storedState, LeaseLength: 0)
            );
        });
        await state.Save();
            
        (await store.GetFunction(FunctionId))!
            .State
            .StateJson!
            .DeserializeFromJsonTo<WorkflowState>()!
            .Name!
            .ShouldBe("Peter");
    }

    public abstract Task ConcreteStateTypeIsUsedWhenSpecifiedAtRegistration();
    protected async Task ConcreteStateTypeIsUsedWhenSpecifiedAtRegistration(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = new FunctionId(
            functionTypeId: nameof(ConcreteStateTypeIsUsedWhenSpecifiedAtRegistration),
            functionInstanceId: "instance"
        );
        var synced = new Synced<ParentState>();
        var rAction = functionsRegistry.RegisterAction<string, ParentState>(
            functionId.TypeId,
            (_, state) => synced.Value = state
        ).Invoke;

        await rAction("instance", "param", new ChildState());

        await BusyWait.UntilAsync(() => synced.Value != null);
        synced.Value.ShouldBeOfType<ChildState>();
    }

    private class ParentState : Domain.WorkflowState { }
    private class ChildState : ParentState { }

    public abstract Task ChangesToStateDictionaryArePersisted();
    protected async Task ChangesToStateDictionaryArePersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        
        var rAction = functionsRegistry.RegisterAction<string, Domain.WorkflowState>(
            nameof(ChangesToStateDictionaryArePersisted),
            (_, state) => state.StateDictionary["hello"] = "world"
        ).Invoke;

        await rAction.Invoke("instance", "test");
        var sf = await store.GetFunction(
            new FunctionId(nameof(ChangesToStateDictionaryArePersisted), "instance")
        ).ShouldNotBeNullAsync();

        var state = sf.State.Deserialize<Domain.WorkflowState>(DefaultSerializer.Instance);
        state.StateDictionary.Count.ShouldBe(1);
        state.StateDictionary["hello"].ShouldBe("world");
    }
}