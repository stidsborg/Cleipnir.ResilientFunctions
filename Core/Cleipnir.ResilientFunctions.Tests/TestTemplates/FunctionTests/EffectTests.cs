using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class EffectTests
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
                var (effect, _) = workflow;
                await effect.Capture(
                    () => syncedCounter.Increment()
                );
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");

        var storedId = rAction.MapToStoredId(flowId.Instance);
        await BusyWait.Until(() =>
            store.GetFunction(storedId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        effectResults.Single(r => r.EffectId == 0.ToEffectId()).WorkStatus.ShouldBe(WorkStatus.Completed);

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Restart();
        
        effectResults = await store.EffectsStore.GetEffectResults(storedId);
        effectResults.Single(r => r.EffectId == 0.ToEffectId()).WorkStatus.ShouldBe(WorkStatus.Completed);
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
                var (effect, _) = workflow;
                await effect.Capture(
                    () => { syncedCounter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");

        var storedId = rAction.MapToStoredId(flowId.Instance);
        await BusyWait.Until(() =>
            store.GetFunction(storedId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        effectResults.Single(r => r.EffectId == 0.ToEffectId()).WorkStatus.ShouldBe(WorkStatus.Completed);

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Restart();
        
        effectResults = await store.EffectsStore.GetEffectResults(storedId);
        effectResults.Single(r => r.EffectId == 0.ToEffectId()).WorkStatus.ShouldBe(WorkStatus.Completed);
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
                var (effect, _) = workflow;
                await effect.Capture(
                    () =>
                    {
                        syncedCounter.Increment();
                        return param;
                    });
            });

        await rAction.Schedule(flowInstance.ToString(), param: "hello");

        var storedId = rAction.MapToStoredId(flowId.Instance);
        await BusyWait.Until(() =>
            store.GetFunction(storedId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        var storedEffect = effectResults.Single(r => r.EffectId == 0.ToEffectId());
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello");

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Restart();
        
        effectResults = await store.EffectsStore.GetEffectResults(storedId);
        storedEffect = effectResults.Single(r => r.EffectId == 0.ToEffectId());
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
                var (effect, _) = workflow;
                await effect.Capture(
                    () =>
                    {
                        syncedCounter.Increment();
                        return param.ToTask();
                    });
            });

        await rAction.Schedule(flowInstance.ToString(), param: "hello");
        
        var storedId = rAction.MapToStoredId(flowId.Instance);
        await BusyWait.Until(() =>
            store.GetFunction(storedId).SelectAsync(sf => sf?.Status == Status.Succeeded)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        var storedEffect = effectResults.Single(r => r.EffectId == 0.ToEffectId());
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello");

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.Restart();
        
        effectResults = await store.EffectsStore.GetEffectResults(storedId);
        storedEffect = effectResults.Single(r => r.EffectId == 0.ToEffectId());
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
                var (effect, _) = workflow;
                await effect.Capture(
                    () =>
                    {
                        syncedCounter.Increment();
                        throw new InvalidOperationException("oh no");
                    });
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");

        var storedId = rAction.MapToStoredId(flowId.Instance);
        await BusyWait.Until(() =>
            store.GetFunction(storedId).SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        syncedCounter.Current.ShouldBe(1);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        var storedEffect = effectResults.Single(r => r.EffectId == 0.ToEffectId());
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Failed);
        storedEffect.StoredException.ShouldNotBeNull();
        storedEffect.StoredException.ExceptionType.ShouldContain("InvalidOperationException");

        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();
        await Should.ThrowAsync<FatalWorkflowException>(() => controlPanel.Restart());

        effectResults = await store.EffectsStore.GetEffectResults(storedId);
        storedEffect = effectResults.Single(r => r.EffectId == 0.ToEffectId());
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
                var (effect, _) = workflow;
                var t1 = new Task<int>(() => 1);
                var t2 = Task.FromResult(2);
                return await effect.WhenAny(t1, t2);
            });

        var result = await rAction.Invoke(flowInstance.ToString(), param: "hello");
        result.ShouldBe(2);
        
        var storedId = rAction.MapToStoredId(flowId.Instance);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        var storedEffect = effectResults.Single(r => r.EffectId == 0.ToEffectId());
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
                var (effect, _) = workflow;
                var t1 = Task.FromResult(1);
                var t2 = Task.FromResult(2);
                return await effect.WhenAll(t1, t2);
            });

        var result = await rAction.Invoke(flowInstance.ToString(), param: "hello");
        result.ShouldBe(new [] { 1, 2 });
        
        var storedId = rAction.MapToStoredId(flowId.Instance);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        var storedEffect = effectResults.Single(r => r.EffectId == 0.ToEffectId());
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<int[]>().ShouldBe(new [] {1, 2});
    }
    
    public abstract Task ClearEffectsTest();
    public async Task ClearEffectsTest(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        
        var registration = functionsRegistry.RegisterAction(
            flowType,
            async Task (string param, Workflow workflow) =>
            {
                var (effect, _) = workflow;
                await effect.Clear("SomeEffect".GetHashCode());
            });

        await store.CreateFunction(
            registration.MapToStoredId(flowId.Instance), 
            "humanInstanceId",
            Test.SimpleStoredParameter,
            leaseExpiration: (DateTime.UtcNow + TimeSpan.FromMinutes(10)).Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );
        
        var controlPanel = await registration.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();

        await controlPanel.Effects.SetSucceeded("SomeEffect".GetHashCode());
        await controlPanel.Restart();

        await controlPanel.Refresh();
        (await controlPanel.Effects.AllIds).Count().ShouldBe(0);
    }
    
    public abstract Task EffectsCrudTest();
    public async Task EffectsCrudTest(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var session = await store.CreateFunction(
            storedId,
            "SomeInstance",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            lazyExistingEffects: new Lazy<Task<IReadOnlyList<StoredEffect>>>(() => store.EffectsStore.GetEffectResults(storedId)),
            store.EffectsStore,
            DefaultSerializer.Instance, 
            session
        );
        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowMinimumTimeout());
        
        var option = await effect.TryGet<int>(1);
        option.HasValue.ShouldBeFalse();

        Should.Throw<InvalidOperationException>(() => effect.Get<int>(1));

        var result = await effect.CreateOrGet(1, 32);
        result.ShouldBe(32);
        result = await effect.CreateOrGet(1, 100);
        result.ShouldBe(32);

        option = await effect.TryGet<int>(1);
        option.HasValue.ShouldBeTrue();
        var value2 = option.Value;
        value2.ShouldBe(32);
        (await effect.Get<int>(1)).ShouldBe(32);

        await effect.Upsert(1, 100);
        (await effect.Get<int>(1)).ShouldBe(100);
        await effect.GetStatus(1).ShouldBeAsync(WorkStatus.Completed);
        await effect.Contains(1).ShouldBeTrueAsync();
    }
    
    public abstract Task ExistingEffectsFuncIsOnlyInvokedAfterGettingValue();
    public async Task ExistingEffectsFuncIsOnlyInvokedAfterGettingValue(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var syncedCounter = new SyncedCounter();

        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            lazyExistingEffects: new Lazy<Task<IReadOnlyList<StoredEffect>>>(
                () =>
                {
                    syncedCounter.Increment();
                    return new List<StoredEffect>()
                        .CastTo<IReadOnlyList<StoredEffect>>()
                        .ToTask();
                })
            ,
            store.EffectsStore,
            DefaultSerializer.Instance, 
            storageSession: null
        );
        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowMinimumTimeout());
        
        syncedCounter.Current.ShouldBe(0);
        
        await effect.TryGet<int>(1);
        syncedCounter.Current.ShouldBe(1);

        await effect.TryGet<int>(1);
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SubEffectHasImplicitContext();
    public async Task SubEffectHasImplicitContext(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var rAction = functionsRegistry.RegisterParamless(
            flowType,
            async Task (workflow) =>
            {
                var effect = workflow.Effect;
                await effect.Capture(async () =>
                {
                    var e1 =  effect.Capture(async () =>
                    {
                        await Task.Delay(10);
                        await effect.Upsert("SubEffectValue1".GetHashCode(), "some value");
                    });
                    await e1;
                    var e2 = effect.Capture(async () =>
                    {
                        await Task.Delay(1);
                        await effect.Upsert("SubEffectValue2".GetHashCode(), "some other value");
                    });

                    await Task.WhenAll(e1, e2);
                });
            }
        );

        await rAction.Invoke(flowInstance.ToString());
        
        var storedId = rAction.MapToStoredId(flowId.Instance);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);

        var subEffectValue1Id = effectResults.Single(se => se.EffectId.Id == "SubEffectValue1".GetHashCode()).EffectId;
        subEffectValue1Id.Context.ShouldBe(new int[] {0, 0});

        var subEffectValue2Id = effectResults.Single(se => se.EffectId.Id == "SubEffectValue2".GetHashCode()).EffectId;
        subEffectValue2Id.Context.ShouldBe(new int[] {0, 1});
    }
    
    public abstract Task SubEffectHasExplicitContext();
    public async Task SubEffectHasExplicitContext(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var rAction = functionsRegistry.RegisterParamless(
            flowType,
            async Task (workflow) =>
            {
                var effect = workflow.Effect;
                await effect.Capture(async () =>
                {
                    var e1 =  effect.Capture(async () =>
                    {
                        await Task.Delay(10);
                        await effect.Upsert("SubEffectValue1".GetHashCode(), "some value");
                    });
                    await e1;
                    var e2 = effect.Capture(async () =>
                    {
                        await Task.Delay(1);
                        await effect.Upsert("SubEffectValue2".GetHashCode(), "some other value");
                    });

                    await Task.WhenAll(e1, e2);
                });
            }
        );

        await rAction.Invoke(flowInstance.ToString());
        
        var storedId = rAction.MapToStoredId(flowId.Instance);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);

        var subEffectValue1Id = effectResults.Single(se => se.EffectId.Id == "SubEffectValue1".GetHashCode()).EffectId;
        subEffectValue1Id.Context.ShouldBe(new int[] {0, 0});

        var subEffectValue2Id = effectResults.Single(se => se.EffectId.Id == "SubEffectValue2".GetHashCode()).EffectId;
        subEffectValue2Id.Context.ShouldBe(new int[] {0, 1});
    }

    public abstract Task ExceptionThrownInsideEffectBecomesFatalWorkflowException();
    public async Task ExceptionThrownInsideEffectBecomesFatalWorkflowException(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var rAction = functionsRegistry.RegisterFunc<string, bool>(
            flowType,
            async Task<bool> (_, workflow) =>
            {
                var effect = workflow.Effect;
                try
                {
                    await effect.Capture(() => throw new InvalidOperationException());
                }
                catch (FatalWorkflowException<InvalidOperationException>)
                {
                    return true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }

                return false;
            }
        );

        var result = await rAction.Invoke(flowInstance.ToString(), "");
        result.ShouldBeTrue();
    }
    
    public abstract Task ExceptionThrownInsideEffectStaysFatalWorkflowException();
    public async Task ExceptionThrownInsideEffectStaysFatalWorkflowException(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var rAction = functionsRegistry.RegisterParamless(
            flowType,
            async Task (workflow) => await workflow.Effect.Capture(() => throw new InvalidOperationException())
        );

        try
        {
            await rAction.Invoke(flowInstance.ToString());
        }
        catch (FatalWorkflowException<InvalidOperationException>)
        {
            return;
        }
        Assert.Fail("Expected FatalWorkflowException<InvalidOperationException>");
        
        try
        {
            await rAction.Invoke(flowInstance.ToString());
        }
        catch (FatalWorkflowException<InvalidOperationException>)
        {
            return;
        }
        Assert.Fail("Expected FatalWorkflowException<InvalidOperationException>");
    }
    
    public abstract Task EffectCanReturnOption();
    public async Task EffectCanReturnOption(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var rAction = functionsRegistry.RegisterFunc(
            flowType,
            async Task<Option<string>> (string message, Workflow workflow) =>
            {
                var (effect, _) = workflow;
                return await effect.Capture(
                    () => Option.Create(message)
                );
            });

        var result = await rAction.Invoke(flowInstance.ToString(), "Hello!");
        result.HasValue.ShouldBeTrue();
        result.Value.ShouldBe("Hello!");
        
        var controlPanel = await rAction.ControlPanel(flowId.Instance);
        controlPanel.ShouldNotBeNull();

        var effectValue = await controlPanel.Effects.GetValue<Option<string>>(0);
        effectValue.ShouldNotBeNull();
        effectValue.HasValue.ShouldBeTrue();
        effectValue.Value.ShouldBe("Hello!");
        
        result = await controlPanel.Restart();
        result.HasValue.ShouldBeTrue();
        result.Value.ShouldBe("Hello!");
    }
    
    public abstract Task DelayedFlushIsReflectedInUnderlyingStoreForSet();
    public async Task DelayedFlushIsReflectedInUnderlyingStoreForSet(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var session = await store.CreateFunction(
            storedId,
            "SomeInstance",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var effectStore = store.EffectsStore;
        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            lazyExistingEffects: new Lazy<Task<IReadOnlyList<StoredEffect>>>(
                () => new List<StoredEffect>().CastTo<IReadOnlyList<StoredEffect>>().ToTask()
            ),
            effectStore,
            DefaultSerializer.Instance, 
            session
        );
        
        var effectId1 = new EffectId([1]);
        var storedEffect1 = new StoredEffect(
            effectId1,
            WorkStatus.Completed,
            Result: "hello world".ToUtf8Bytes(),
            StoredException: null,
            Alias: null
        );
        await effectResults.Set(storedEffect1, flush: false);
        await effectStore
            .GetEffectResults(storedId)
            .SelectAsync(r => r.Count == 0)
            .ShouldBeTrueAsync();
        
        var effectId2 = new EffectId([2]);
        var storedEffect2 = new StoredEffect(
            effectId2,
            WorkStatus.Completed,
            Result: "hello universe".ToUtf8Bytes(),
            StoredException: null,
            Alias: null
        );
        await effectResults.Set(storedEffect2, flush: true);
        
        var fetchedResults = await effectStore.GetEffectResults(storedId);
        fetchedResults.Count.ShouldBe(2);
        fetchedResults
            .Single(r => r.EffectId == effectId1)
            .Result!
            .ToStringFromUtf8Bytes()
            .ShouldBe("hello world");
        fetchedResults
            .Single(r => r.EffectId == effectId2)
            .Result!
            .ToStringFromUtf8Bytes()
            .ShouldBe("hello universe");
    }
    
    public abstract Task CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlush();
    public async Task CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlush(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var session = await store.CreateFunction(
            storedId,
            "SomeInstance",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );

        var effectStore = store.EffectsStore;
        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            lazyExistingEffects: new Lazy<Task<IReadOnlyList<StoredEffect>>>(
                () => new List<StoredEffect>().CastTo<IReadOnlyList<StoredEffect>>().ToTask()
            ),
            effectStore,
            DefaultSerializer.Instance, 
            session
        );
        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowMinimumTimeout());

        var result = await effect.Capture(() => "hello world", ResiliencyLevel.AtLeastOnceDelayFlush);
        result.ShouldBe("hello world");

        await effectStore.GetEffectResults(storedId).ShouldBeEmptyAsync();

        await effect.Capture(() => "hello universe");

        var storedEffects = await effectStore.GetEffectResults(storedId);
        storedEffects.Count.ShouldBe(2);
        storedEffects.Single(se => se.EffectId.Id == 0).Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello world");
        storedEffects.Single(se => se.EffectId.Id == 1).Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello universe");
    }

    public abstract Task CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlushInFlow();
    public async Task CaptureUsingAtLeastOnceWithoutFlushResiliencyDelaysFlushInFlow(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        var (type, instance) = TestFlowId.Create();
        var storedId = TestStoredId.Create();
        var someEffectIdValue = Guid.NewGuid();

        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var registry = new FunctionsRegistry(store, settings: new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch));

        var writtenEffectFlag = new SyncedFlag();
        var continueFlag = new SyncedFlag();
        
        var flow = registry.RegisterParamless(
            type,
            async Task (workflow) =>
            {
                await workflow.Effect.Capture(() => someEffectIdValue, ResiliencyLevel.AtLeastOnceDelayFlush);
                writtenEffectFlag.Raise();
                await continueFlag.WaitForRaised();
            }
        );

        await flow.Schedule(instance);
        
        await writtenEffectFlag.WaitForRaised();
        var cp = await flow.ControlPanel(instance).ShouldNotBeNullAsync();
        var effectIds = await cp.Effects.AllIds;
        effectIds.Any().ShouldBeFalse();
        
        continueFlag.Raise();

        await cp.WaitForCompletion();

        await cp.Refresh();
        effectIds = (await cp.Effects.AllIds).ToList();
        effectIds.Count().ShouldBe(1);
        effectIds.Single().Id.ShouldBe(0);
        (await cp.Effects.GetValue<Guid>(0)).ShouldBe(someEffectIdValue);
    }
 
    public abstract Task UpsertingExistingEffectDoesNotAffectOtherExistingEffects();
    public async Task UpsertingExistingEffectDoesNotAffectOtherExistingEffects(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        var storedId = TestStoredId.Create();
        var session = await store.CreateFunction(
            storedId,
            "SomeInstance",
            param: null,
            leaseExpiration: 0,
            postponeUntil: null,
            timestamp: 0,
            parent: null,
            owner: ReplicaId.NewId()
        );
        var effectStore = store.EffectsStore;
        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            lazyExistingEffects: new Lazy<Task<IReadOnlyList<StoredEffect>>>(
                () => new List<StoredEffect>().CastTo<IReadOnlyList<StoredEffect>>().ToTask()
            ),
            effectStore,
            DefaultSerializer.Instance, 
            session
        );
        var effect = new Effect(effectResults, utcNow: () => DateTime.UtcNow, new FlowMinimumTimeout());

        await effect.Capture(() => "hello world");
        await effect.Capture(() => "hello universe");
        await effect.Flush();

        await effect.Upsert(0, "hello world again");

        var storedEffects = await effectStore.GetEffectResults(storedId);
        storedEffects.Count.ShouldBe(2);
        storedEffects.Single(se => se.EffectId.Id == 0).Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello world again");
        storedEffects.Single(se => se.EffectId.Id == 1).Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello universe");
    }
    
    public abstract Task CaptureEffectWithRetryPolicy();
    public async Task CaptureEffectWithRetryPolicy(Task<IFunctionStore> storeTask)
    {
        var utcNow = DateTime.UtcNow;
        
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        using var registry = new FunctionsRegistry(store, new Settings(utcNow: () => utcNow, enableWatchdogs: false));
        var syncedCounter = new SyncedCounter();

        var retryPolicy = RetryPolicy.Create(suspendThreshold: TimeSpan.Zero, initialInterval: TimeSpan.FromSeconds(1), backoffCoefficient: 1);
        var registration = registry.RegisterParamless(
            flowType: flowId.Type,
            async workflow =>
            {
                var effect = workflow.Effect;
                await effect.Capture(() =>
                {
                    if (syncedCounter.Current <= 1)
                    {
                        syncedCounter.Increment();
                        throw new TimeoutException();
                    } 
                    
                    syncedCounter.Increment();
                    return Task.CompletedTask;
                }, retryPolicy);
            }
        );

        await Should.ThrowAsync<InvocationPostponedException>(() => registration.Invoke(flowId.Instance));
        utcNow += TimeSpan.FromSeconds(2);
        
        var cp = await registration.ControlPanel(flowId.Instance).ShouldNotBeNullAsync();
        cp.Status.ShouldBe(Status.Postponed);
        
        await Should.ThrowAsync<InvocationPostponedException>(() => cp.Restart());
        
        utcNow += TimeSpan.FromSeconds(10);
        
        await cp.Restart();
        
        syncedCounter.Current.ShouldBe(3);
    }
    
    public abstract Task CaptureEffectWithRetryPolicyWithResult();
    public async Task CaptureEffectWithRetryPolicyWithResult(Task<IFunctionStore> storeTask)
    {
        var utcNow = DateTime.UtcNow;
        
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        using var registry = new FunctionsRegistry(store, new Settings(utcNow: () => utcNow, enableWatchdogs: false));
        var syncedCounter = new SyncedCounter();

        var retryPolicy = RetryPolicy.Create(suspendThreshold: TimeSpan.Zero, initialInterval: TimeSpan.FromSeconds(1), backoffCoefficient: 1);
        var registration = registry.RegisterFunc<string, string>(
            flowType: flowId.Type,
            async (param, workflow) =>
            {
                var effect = workflow.Effect;
                return await effect.Capture(() =>
                {
                    if (syncedCounter.Current <= 1)
                    {
                        syncedCounter.Increment();
                        throw new TimeoutException();
                    } 
                    
                    syncedCounter.Increment();
                    return Task.FromResult(param);
                }, retryPolicy);
            }
        );

        await Should.ThrowAsync<InvocationPostponedException>(() => registration.Invoke(flowId.Instance, "Hello World!"));
        utcNow += TimeSpan.FromSeconds(2);
        
        var cp = await registration.ControlPanel(flowId.Instance).ShouldNotBeNullAsync();
        cp.Status.ShouldBe(Status.Postponed);
        
        await Should.ThrowAsync<InvocationPostponedException>(() => cp.Restart());
        
        utcNow += TimeSpan.FromSeconds(10);
        
        var result = await cp.Restart();
        result.ShouldBe("Hello World!");
        
        syncedCounter.Current.ShouldBe(3);
    }
    
    public abstract Task CaptureEffectWithRetryPolicyWithoutSuspension();
    public async Task CaptureEffectWithRetryPolicyWithoutSuspension(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        using var registry = new FunctionsRegistry(store);
        var syncedCounter = new SyncedCounter();

        var retryPolicy = RetryPolicy.Create(suspendThreshold: TimeSpan.MaxValue, initialInterval: TimeSpan.FromMilliseconds(100), backoffCoefficient: 1);
        var registration = registry.RegisterFunc<string, string>(
            flowType: flowId.Type,
            async (param, workflow) =>
            {
                var effect = workflow.Effect;
                return await effect.Capture(() =>
                {
                    if (syncedCounter.Current < 2)
                    {
                        syncedCounter.Increment();
                        throw new TimeoutException();
                    } 
                    
                    syncedCounter.Increment();
                    return Task.FromResult(param);
                }, retryPolicy);
            }
        );

        var result = await registration.Invoke(flowId.Instance, "Hello World!");
        result.ShouldBe("Hello World!");
        
        syncedCounter.Current.ShouldBe(3);
    }
    
    public abstract Task ExceptionPredicateIsUsedForRetryPolicy();
    public async Task ExceptionPredicateIsUsedForRetryPolicy(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create();
        using var registry = new FunctionsRegistry(store);
        var syncedCounter = new SyncedCounter();

        var retryPolicy = RetryPolicy.Create(
            suspendThreshold: TimeSpan.MaxValue,
            initialInterval: TimeSpan.FromMilliseconds(100),
            backoffCoefficient: 1,
            shouldRetry: e => e is TimeoutException
        );
        var registration = registry.RegisterParamless(
            flowType: flowId.Type,
            async workflow =>
            {
                var effect = workflow.Effect;
                await effect.Capture(() =>
                {
                    if (syncedCounter.Current == 0)
                    {
                        syncedCounter.Increment();
                        throw new TimeoutException();
                    }
                    else
                    {
                        syncedCounter.Increment();
                        throw new InvalidOperationException();
                    }
                }, retryPolicy);
            }
        );

        try
        {
            await registration.Invoke(flowId.Instance);
            Assert.Fail("Expected InvalidOperationException");
        }
        catch (FatalWorkflowException e)
        {
            e.ErrorType.ShouldBe(typeof(InvalidOperationException));
        }
        
        syncedCounter.Current.ShouldBe(2);
    }
}