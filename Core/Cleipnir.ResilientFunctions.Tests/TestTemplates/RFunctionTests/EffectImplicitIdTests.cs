using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class EffectImplicitIdTests
{
    public abstract Task SunshineActionTest();
    public async Task SunshineActionTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(syncedCounter.Increment);
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(flowId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(flowId);
        effectResults.Single(r => r.EffectId == "0").WorkStatus.ShouldBe(WorkStatus.Completed);

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Restart();
        
        effectResults = await store.EffectsStore.GetEffectResults(flowId);
        effectResults.Single(r => r.EffectId == "0").WorkStatus.ShouldBe(WorkStatus.Completed);
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SunshineAsyncActionTest();
    public async Task SunshineAsyncActionTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(() => { syncedCounter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(flowId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(flowId);
        effectResults.Single(r => r.EffectId == "0").WorkStatus.ShouldBe(WorkStatus.Completed);

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Restart();
        
        effectResults = await store.EffectsStore.GetEffectResults(flowId);
        effectResults.Single(r => r.EffectId == "0").WorkStatus.ShouldBe(WorkStatus.Completed);
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SunshineFuncTest();
    public async Task SunshineFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(
                    () =>
                    {
                        syncedCounter.Increment();
                        return param;
                    });
            });

        await rAction.Schedule(flowInstance.ToString(), param: "hello");

        await BusyWait.Until(() =>
            store.GetFunction(flowId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(flowId);
        var storedEffect = effectResults.Single(r => r.EffectId == "0");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello");

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Restart();
        
        effectResults = await store.EffectsStore.GetEffectResults(flowId);
        storedEffect = effectResults.Single(r => r.EffectId == "0");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello");
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SunshineAsyncFuncTest();
    public async Task SunshineAsyncFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(
                    work: () =>
                    {
                        syncedCounter.Increment();
                        return param.ToTask();
                    });
            });

        await rAction.Schedule(flowInstance.ToString(), param: "hello");

        await BusyWait.Until(() =>
            store.GetFunction(flowId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(flowId);
        var storedEffect = effectResults.Single(r => r.EffectId == "0");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello");

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Restart();
        
        effectResults = await store.EffectsStore.GetEffectResults(flowId);
        storedEffect = effectResults.Single(r => r.EffectId == "0");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello");
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task ExceptionThrowingActionTest();
    public async Task ExceptionThrowingActionTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var syncedCounter = new SyncedCounter();
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                await effect.Capture(
                    work: () =>
                    {
                        syncedCounter.Increment();
                        throw new InvalidOperationException("oh no");
                    });
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(flowId).SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(flowId);
        var storedEffect = effectResults.Single(r => r.EffectId == "0");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Failed);
        storedEffect.StoredException.ShouldNotBeNull();
        storedEffect.StoredException.ExceptionType.ShouldContain("InvalidOperationException");

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await Should.ThrowAsync<EffectException>(() => controlPanel.Restart());
        
        effectResults = await store.EffectsStore.GetEffectResults(flowId);
        storedEffect = effectResults.Single(r => r.EffectId == "0");
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
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var rAction = functionsRegistry.RegisterFunc(
            flowType,
            async Task<int> (string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                var t1 = new Task<int>(() => 1);
                var t2 = Task.FromResult(2);
                return await effect.WhenAny(t1, t2);
            });

        var result = await rAction.Invoke(flowInstance.ToString(), param: "hello");
        result.ShouldBe(2);
        
        var effectResults = await store.EffectsStore.GetEffectResults(flowId);
        var storedEffect = effectResults.Single(r => r.EffectId == "0");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<int>().ShouldBe(2);
    }
    
    public abstract Task TaskWhenAllFuncTest();
    public async Task TaskWhenAllFuncTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var rAction = functionsRegistry.RegisterFunc(
            flowType,
            async Task<int[]> (string param, Workflow workflow) =>
            {
                var (effect, _, _) = workflow;
                var t1 = Task.FromResult(1);
                var t2 = Task.FromResult(2);
                return await effect.WhenAll(t1, t2);
            });

        var result = await rAction.Invoke(flowInstance.ToString(), param: "hello");
        result.ShouldBe(new [] { 1, 2 });
        
        var effectResults = await store.EffectsStore.GetEffectResults(flowId);
        var storedEffect = effectResults.Single(r => r.EffectId == "0");
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<int[]>().ShouldBe(new [] {1, 2});
    }
    
    public abstract Task MultipleEffectsTest();
    public async Task MultipleEffectsTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var registration = functionsRegistry.RegisterAction(
            flowType,
            async Task(string _, Workflow workflow) =>
            {
                var effect = workflow.Effect;
                await effect.Capture(() => "0");
                await effect.Capture(() => "1");
            });

        await registration.Invoke(flowInstance.ToString(), "hello");
        
        var controlPanel = await registration.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Restart();
        await controlPanel.Refresh();

        var effects = controlPanel.Effects;
        var effectIds = (await effects.AllIds).ToList();
        effectIds.Count.ShouldBe(2);
        effectIds.Any(id => id == "0").ShouldBeTrue();
        effectIds.Any(id => id == "1").ShouldBeTrue();

        await effects.GetValue<string>("0").ShouldBeAsync("0");
        await effects.GetValue<string>("1").ShouldBeAsync("1");
    }
}