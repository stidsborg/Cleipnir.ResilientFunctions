using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class EffectTests
{
    public abstract Task SunshineActionTest();
    public async Task SunshineActionTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(
                    id: "Test",
                    work: () => syncedCounter.Increment()
                );
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(functionId);
        effectResults.Single(r => r.EffectId == "Test").WorkStatus.ShouldBe(WorkStatus.Completed);

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await controlPanel.ReInvoke();
        
        effectResults = await store.EffectsStore.GetEffectResults(functionId);
        effectResults.Single(r => r.EffectId == "Test").WorkStatus.ShouldBe(WorkStatus.Completed);
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SunshineAsyncActionTest();
    public async Task SunshineAsyncActionTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(
                    id: "Test",
                    work: () => { syncedCounter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(functionId);
        effectResults.Single(r => r.EffectId == "Test").WorkStatus.ShouldBe(WorkStatus.Completed);

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await controlPanel.ReInvoke();
        
        effectResults = await store.EffectsStore.GetEffectResults(functionId);
        effectResults.Single(r => r.EffectId == "Test").WorkStatus.ShouldBe(WorkStatus.Completed);
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SunshineFuncTest();
    public async Task SunshineFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(
                    id: "Test",
                    work: () =>
                    {
                        syncedCounter.Increment();
                        return param;
                    });
            });

        await rAction.Schedule(functionInstanceId.ToString(), param: "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(functionId);
        var storedEffect = effectResults.Single(r => r.EffectId == "Test");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.DeserializeFromJsonTo<string>().ShouldBe("hello");

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await controlPanel.ReInvoke();
        
        effectResults = await store.EffectsStore.GetEffectResults(functionId);
        storedEffect = effectResults.Single(r => r.EffectId == "Test");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.DeserializeFromJsonTo<string>().ShouldBe("hello");
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SunshineAsyncFuncTest();
    public async Task SunshineAsyncFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(
                    id: "Test",
                    work: () =>
                    {
                        syncedCounter.Increment();
                        return param.ToTask();
                    });
            });

        await rAction.Schedule(functionInstanceId.ToString(), param: "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(functionId);
        var storedEffect = effectResults.Single(r => r.EffectId == "Test");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.DeserializeFromJsonTo<string>().ShouldBe("hello");

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await controlPanel.ReInvoke();
        
        effectResults = await store.EffectsStore.GetEffectResults(functionId);
        storedEffect = effectResults.Single(r => r.EffectId == "Test");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.DeserializeFromJsonTo<string>().ShouldBe("hello");
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task ExceptionThrowingActionTest();
    public async Task ExceptionThrowingActionTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(
                    id: "Test",
                    work: () =>
                    {
                        syncedCounter.Increment();
                        throw new InvalidOperationException("oh no");
                    });
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(functionId);
        var storedEffect = effectResults.Single(r => r.EffectId == "Test");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Failed);
        storedEffect.StoredException.ShouldNotBeNull();
        storedEffect.StoredException.ExceptionType.ShouldContain("InvalidOperationException");

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();
        await Should.ThrowAsync<EffectException>(() => controlPanel.ReInvoke());
        
        effectResults = await store.EffectsStore.GetEffectResults(functionId);
        storedEffect = effectResults.Single(r => r.EffectId == "Test");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Failed);
        storedEffect.StoredException.ShouldNotBeNull();
        storedEffect.StoredException.ExceptionType.ShouldContain("InvalidOperationException");
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task TaskWhenAnyFuncTest();
    public async Task TaskWhenAnyFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var rAction = functionsRegistry.RegisterFunc(
            functionTypeId,
            async Task<int> (string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                var t1 = new Task<int>(() => 1);
                var t2 = Task.FromResult(2);
                return await effect.WhenAny("WhenAny", t1, t2);
            });

        var result = await rAction.Invoke(functionInstanceId.ToString(), param: "hello");
        result.ShouldBe(2);
        
        var effectResults = await store.EffectsStore.GetEffectResults(functionId);
        var storedEffect = effectResults.Single(r => r.EffectId == "WhenAny");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.DeserializeFromJsonTo<int>().ShouldBe(2);
    }
    
    public abstract Task TaskWhenAllFuncTest();
    public async Task TaskWhenAllFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var rAction = functionsRegistry.RegisterFunc(
            functionTypeId,
            async Task<int[]> (string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                var t1 = Task.FromResult(1);
                var t2 = Task.FromResult(2);
                return await effect.WhenAll("WhenAll", t1, t2);
            });

        var result = await rAction.Invoke(functionInstanceId.ToString(), param: "hello");
        result.ShouldBe(new [] { 1, 2 });
        
        var effectResults = await store.EffectsStore.GetEffectResults(functionId);
        var storedEffect = effectResults.Single(r => r.EffectId == "WhenAll");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.DeserializeFromJsonTo<int[]>().ShouldBe(new [] {1, 2});
    }
    
    public abstract Task ClearEffectsTest();
    public async Task ClearEffectsTest(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;

        await store.CreateFunction(
            functionId,
            Test.SimpleStoredParameter,
            leaseExpiration: (DateTime.UtcNow + TimeSpan.FromMinutes(10)).Ticks,
            postponeUntil: null,
            timestamp: DateTime.Now.Ticks
        );
        
        var registration = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task (string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Clear("SomeEffect");
            });

        var controlPanel = await registration.ControlPanel(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Effects.SetSucceeded("SomeEffect");
        await controlPanel.ReInvoke();

        await controlPanel.Refresh();
        controlPanel.Effects.All.Count.ShouldBe(0);
    }
    
    public abstract Task EffectsCrudTest();
    public async Task EffectsCrudTest(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var effect = new Effect(
            functionId,
            existingEffects: Array.Empty<StoredEffect>(),
            store.EffectsStore,
            DefaultSerializer.Instance
        );
        
        var success = effect.TryGet("Id1", out int value);
        success.ShouldBeFalse();
                
        Should.Throw<InvalidOperationException>(() => effect.Get<int>("Id1"));

        var result = await effect.CreateOrGet("Id1", 32);
        result.ShouldBe(32);
        result = await effect.CreateOrGet("Id1", 100);
        result.ShouldBe(32);
                
        success = effect.TryGet("Id1", out int value2);
        success.ShouldBeTrue();
        value2.ShouldBe(32);
        effect.Get<int>("Id1").ShouldBe(32);
                
        await effect.Upsert("Id1", 100);
        effect.Get<int>("Id1").ShouldBe(100);
    }
}