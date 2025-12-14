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
using Cleipnir.ResilientFunctions.Reactive.Extensions;
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
                return await effect.Capture(async () => await await Task.WhenAny(t1, t2));
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
                return await effect.Capture(() => Task.WhenAll(t1, t2));
            });

        var result = await rAction.Invoke(flowInstance.ToString(), param: "hello");
        result.ShouldBe(new [] { 1, 2 });
        
        var storedId = rAction.MapToStoredId(flowId.Instance);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);
        var storedEffect = effectResults.Single(r => r.EffectId == 0.ToEffectId());
        storedEffect.WorkStatus.ShouldBe(WorkStatus.Completed);
        storedEffect.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<int[]>().ShouldBe(new [] {1, 2});
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
        
        var option = await effect.TryGet<int>("alias");
        option.HasValue.ShouldBeFalse();

        Should.Throw<InvalidOperationException>(() => effect.Get<int>("nonexistent"));

        var result = await effect.CreateOrGet("alias", 32);
        result.ShouldBe(32);
        result = await effect.CreateOrGet("alias", 100);
        result.ShouldBe(32);

        option = await effect.TryGet<int>("alias");
        option.HasValue.ShouldBeTrue();
        var value2 = option.Value;
        value2.ShouldBe(32);
        (await effect.Get<int>("alias")).ShouldBe(32);

        await effect.Upsert("alias", 100);
        (await effect.Get<int>("alias")).ShouldBe(100);
        await effect.GetStatus(0).ShouldBeAsync(WorkStatus.Completed);
        await effect.Contains(0).ShouldBeTrueAsync();
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
        
        await effect.TryGet<int>("alias");
        syncedCounter.Current.ShouldBe(1);

        await effect.TryGet<int>("alias");
        syncedCounter.Current.ShouldBe(1);
    }
    
    public abstract Task SubEffectHasImplicitContext();
    public async Task SubEffectHasImplicitContext(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var readFlag = new SyncedFlag();
        var continueFlag = new SyncedFlag();

        var rAction = functionsRegistry.RegisterParamless(
            flowType,
            async Task (workflow) =>
            {
                var effect = workflow.Effect;
                await effect.Capture(async () =>
                {
                    // Create upserts directly before nested captures complete
                    var e1 =  effect.Capture(async () =>
                    {
                        await Task.Delay(10);
                        await effect.Upsert("SubEffectValue1", "some value");
                        await effect.Flush();
                        readFlag.Raise();
                        await continueFlag.WaitForRaised();
                    });
                    await e1;
                });
            }
        );

        var invocation = Task.Run(() => rAction.Invoke(flowInstance.ToString()));

        await readFlag.WaitForRaised();

        var storedId = rAction.MapToStoredId(flowId.Instance);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);

        // Verify child effect exists and has correct context before nested capture completes
        var subEffectValue1 = effectResults.SingleOrDefault(se => se.Alias == "SubEffectValue1");
        subEffectValue1.ShouldNotBeNull();
        subEffectValue1.EffectId.Context.ShouldBe(new int[] {0, 0});

        continueFlag.Raise();
        await invocation;

        // After nested capture completes, child effect should be cleared
        effectResults = await store.EffectsStore.GetEffectResults(storedId);
        effectResults.Any(se => se.Alias == "SubEffectValue1").ShouldBeFalse();
    }

    public abstract Task SubEffectHasExplicitContext();
    public async Task SubEffectHasExplicitContext(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var flowId = TestFlowId.Create();
        var (flowType, flowInstance) = flowId;
        var readFlag = new SyncedFlag();
        var continueFlag = new SyncedFlag();

        var rAction = functionsRegistry.RegisterParamless(
            flowType,
            async Task (workflow) =>
            {
                var effect = workflow.Effect;
                await effect.Capture(async () =>
                {
                    // Create upserts directly before nested captures complete
                    var e2 = effect.Capture(async () =>
                    {
                        await Task.Delay(1);
                        await effect.Upsert("SubEffectValue2", "some other value");
                        await effect.Flush();
                        readFlag.Raise();
                        await continueFlag.WaitForRaised();
                    });
                    await e2;
                });
            }
        );

        var invocation = Task.Run(() => rAction.Invoke(flowInstance.ToString()));

        await readFlag.WaitForRaised();

        var storedId = rAction.MapToStoredId(flowId.Instance);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);

        // Verify child effect exists and has correct context before nested capture completes
        var subEffectValue2 = effectResults.SingleOrDefault(se => se.Alias == "SubEffectValue2");
        subEffectValue2.ShouldNotBeNull();
        subEffectValue2.EffectId.Context.ShouldBe(new int[] {0, 0});

        continueFlag.Raise();
        await invocation;

        // After nested capture completes, child effect should be cleared
        effectResults = await store.EffectsStore.GetEffectResults(storedId);
        effectResults.Any(se => se.Alias == "SubEffectValue2").ShouldBeFalse();
    }

    public abstract Task EffectsHasCorrectlyOrderedIds();
    public async Task EffectsHasCorrectlyOrderedIds(Task<IFunctionStore> storeTask)
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
                var c0 = effect.Capture("0", async () =>
                {
                    var c00 = effect.Capture("0,0", async () =>
                    {
                        await Task.Delay(10);
                        return "0,0";
                    });
                   
                    var c01 = effect.Capture("0,1", async () =>
                    {
                        await Task.Delay(1);
                        return "0,1";
                    });

                    await Task.WhenAll(c00, c01);
                    return "0";
                });
                
                var c1 = effect.Capture("1", async () =>
                {
                    var c10 = effect.Capture("1,0", async () =>
                    {
                        await Task.Delay(10);
                        return "1,0";
                    });
                   
                    var c11 = effect.Capture("1,1", async () =>
                    {
                        await Task.Delay(1);
                        return "1,1";
                    });

                    await Task.WhenAll(c10, c11);
                    return "1";
                });

                await Task.WhenAll(c0, c1);
            }
        );

        await rAction.Invoke(flowInstance);

        var storedId = rAction.MapToStoredId(flowId.Instance);
        var effectResults = await store.EffectsStore.GetEffectResults(storedId);

        foreach (var se in effectResults.Where(e => e.Alias != null))
        {
            var ctx = se.EffectId.ToString();
            var alias = "[" + se.Alias + "]";
            ctx.ShouldBe(alias);
        }
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
            lazyExistingEffects: new Lazy<Task<IReadOnlyList<StoredEffect>>>(() => effectStore.GetEffectResults(storedId)),
            effectStore,
            DefaultSerializer.Instance,
            session
        );
        // Create two effects using the internal API with explicit IDs to avoid implicit ID issues
        await effectResults.CreateOrGet(0.ToEffectId(), "hello world", "first", flush: true);
        await effectResults.CreateOrGet(1.ToEffectId(), "hello universe", "second", flush: true);

        // Upsert the first effect - should not affect the second
        await effectResults.Upsert(0.ToEffectId(), "first", "hello world again", flush: true);

        var storedEffects = await effectStore.GetEffectResults(storedId);
        storedEffects.Count.ShouldBe(2);
        storedEffects.Single(se => se.Alias == "first").Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello world again");
        storedEffects.Single(se => se.Alias == "second").Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().ShouldBe("hello universe");
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
    
    public abstract Task EffectLoopingWorks();
    public async Task EffectLoopingWorks(Task<IFunctionStore> storeTask)
    {  
        var store = await storeTask;
        var id = TestFlowId.Create();
     
        using var registry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)));
        var iterations = new List<int>();
        var flag = new Synced<int>();
        var registration = registry.RegisterParamless(
            id.Type,
            inner: async workflow =>
            {
                await workflow.Effect.Capture(alias: "Before", () => "");
                var elms = new[] { 0, 1, 2, 3, 4, 5 };
                await elms.CaptureEach(
                    async i =>
                    {
                        await workflow.Effect.Capture(alias: i.ToString(), () => i);
                        flag.Value = i;
                        await workflow.Messages.Where(m => m.ToString() == i.ToString()).First();
                        iterations.Add(i);
                    },
                    alias: "Loop"
                );

                await workflow.Delay(TimeSpan.FromMilliseconds(25), alias: "After");
            });

        await registration.Schedule(id.Instance);

        var cp = await registration.ControlPanel(id.Instance).ShouldNotBeNullAsync();
        var messageWriter = registration.MessageWriters.For(id.Instance);
        var effectStore = store.EffectsStore;

        for (var i = 0; i < 6; i++)
        {
            await BusyWait.Until(() => flag.Value == i);
            await cp.BusyWaitUntil(c => c.Status == Status.Suspended);
            var storedEffects = await effectStore.GetEffectResults(registration.MapToStoredId(id.Instance));
            storedEffects.Any(e => e.Alias == "Before").ShouldBeTrue();
            storedEffects.Any(e => e.Alias == "Loop").ShouldBeTrue();
            storedEffects.Single(e => e.Alias == i.ToString()).EffectId.ShouldBe(new EffectId([1,i,0]));
            storedEffects.Count.ShouldBe(3);

            await messageWriter.AppendMessage(i.ToString());
        }

        await cp.BusyWaitUntil(c => c.Status == Status.Succeeded);

        iterations.SequenceEqual([0, 1, 2, 3, 4, 5]).ShouldBeTrue();
    }

    public abstract Task ChildEffectsAreClearedWhenParentEffectWithResultCompletes();
    public async Task ChildEffectsAreClearedWhenParentEffectWithResultCompletes(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var readFlag = new SyncedFlag();
        var continueFlag = new SyncedFlag();

        using var registry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)));
        var registration = registry.RegisterParamless(
            id.Type,
            inner: async workflow =>
            {
                await workflow.Effect.Capture(alias: "Parent", async () =>
                {
                    await workflow.Effect.Capture("Child1", () => "Child1");
                    await workflow.Effect.Capture("Child2", () => "Child2");
                    await workflow.Effect.Flush();
                    readFlag.Raise();
                    await continueFlag.WaitForRaised();
                });
            });

        var invocation = Task.Run(() => registration.Invoke(id.Instance));

        await readFlag.WaitForRaised();

        var effectStore = store.EffectsStore;
        var storedEffects = await effectStore.GetEffectResults(registration.MapToStoredId(id.Instance));
        storedEffects.Any(s => s.Alias == "Child1").ShouldBeTrue();
        storedEffects.Any(s => s.Alias == "Child2").ShouldBeTrue();
        continueFlag.Raise();

        await invocation;

        storedEffects = await effectStore.GetEffectResults(registration.MapToStoredId(id.Instance));
        storedEffects.Count.ShouldBe(1);
        storedEffects.Any(s => s.Alias == "Parent").ShouldBeTrue();
    }

    public abstract Task ChildEffectsAreClearedWhenParentEffectReturningValueCompletes();
    public async Task ChildEffectsAreClearedWhenParentEffectReturningValueCompletes(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var readFlag = new SyncedFlag();
        var continueFlag = new SyncedFlag();

        using var registry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)));
        var registration = registry.RegisterParamless(
            id.Type,
            inner: async workflow =>
            {
                await workflow.Effect.Capture(alias: "Parent", async () =>
                {
                    await workflow.Effect.Capture("Child1", () => "Child1");
                    await workflow.Effect.Capture("Child2", () => "Child2");
                    await workflow.Effect.Flush();
                    readFlag.Raise();
                    await continueFlag.WaitForRaised();
                    return "parent result";
                });
            });

        var invocation = Task.Run(() => registration.Invoke(id.Instance));

        await readFlag.WaitForRaised();

        var effectStore = store.EffectsStore;
        var storedEffects = await effectStore.GetEffectResults(registration.MapToStoredId(id.Instance));
        storedEffects.Any(s => s.Alias == "Child1").ShouldBeTrue();
        storedEffects.Any(s => s.Alias == "Child2").ShouldBeTrue();
        continueFlag.Raise();

        await invocation;

        storedEffects = await effectStore.GetEffectResults(registration.MapToStoredId(id.Instance));
        storedEffects.Count.ShouldBe(1);
        storedEffects.Any(s => s.Alias == "Parent").ShouldBeTrue();
    }

    public abstract Task AggregateEachBasicAggregationWorks();
    public async Task AggregateEachBasicAggregationWorks(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();

        using var registry = new FunctionsRegistry(store);
        var registration = registry.RegisterFunc(
            id.Type,
            async Task<int> (string param, Workflow workflow) =>
            {
                var elms = new[] { 1, 2, 3, 4, 5 };
                var result = await elms.CaptureAggregate(
                    seed: 0,
                    handler: async (elm, acc) =>
                    {
                        await Task.Yield();
                        return acc + elm;
                    },
                    alias: "Aggregate"
                );
                return result;
            });

        var result = await registration.Invoke(id.Instance, "test");
        result.ShouldBe(15);

        var effectStore = store.EffectsStore;
        var storedEffects = await effectStore.GetEffectResults(registration.MapToStoredId(id.Instance));
        storedEffects.Any(e => e.Alias == "Aggregate").ShouldBeTrue();
    }

    public abstract Task AggregateEachResumesMidAggregation();
    public async Task AggregateEachResumesMidAggregation(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();
        var syncedCounter = new SyncedCounter();

        using var registry = new FunctionsRegistry(store);
        var registration = registry.RegisterFunc(
            id.Type,
            async Task<int> (string param, Workflow workflow) =>
            {
                var elms = new[] { 1, 2, 3, 4, 5 };
                var result = await elms.CaptureAggregate(
                    seed: 0,
                    handler: async (elm, acc) =>
                    {
                        syncedCounter.Increment();
                        if (syncedCounter.Current == 3)
                            throw new Exception("Simulated crash");
                        await Task.Yield();
                        return acc + elm;
                    },
                    alias: "Aggregate"
                );
                return result;
            });

        await Should.ThrowAsync<FatalWorkflowException>(() => registration.Invoke(id.Instance, "test"));
        syncedCounter.Current.ShouldBe(3);

        var cp = await registration.ControlPanel(id.Instance).ShouldNotBeNullAsync();
        var result = await cp.Restart(clearFailures: true);
        result.ShouldBe(15);

        // Should have processed elements 3, 4, 5 on restart (elements 1, 2 were already processed)
        syncedCounter.Current.ShouldBe(6);
    }

    public abstract Task AggregateEachWithComplexAccumulator();
    public async Task AggregateEachWithComplexAccumulator(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();

        using var registry = new FunctionsRegistry(store);
        var registration = registry.RegisterFunc(
            id.Type,
            async Task<List<string>> (string param, Workflow workflow) =>
            {
                var elms = new[] { "a", "b", "c", "d" };
                var result = await elms.CaptureAggregate(
                    seed: new List<string>(),
                    handler: async (elm, acc) =>
                    {
                        await Task.Yield();
                        acc.Add(elm.ToUpper());
                        return acc;
                    },
                    alias: "Aggregate"
                );
                return result;
            });

        var result = await registration.Invoke(id.Instance, "test");
        result.SequenceEqual(new[] { "A", "B", "C", "D" }).ShouldBeTrue();

        var cp = await registration.ControlPanel(id.Instance).ShouldNotBeNullAsync();
        var restartResult = await cp.Restart();
        restartResult.SequenceEqual(new[] { "A", "B", "C", "D" }).ShouldBeTrue();
    }

    public abstract Task AggregateEachCleansUpIntermediateEffects();
    public async Task AggregateEachCleansUpIntermediateEffects(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();

        using var registry = new FunctionsRegistry(store);
        var registration = registry.RegisterFunc(
            id.Type,
            async Task<int> (string param, Workflow workflow) =>
            {
                var elms = new[] { 1, 2, 3 };
                var result = await elms.CaptureAggregate(
                    seed: 0,
                    handler: async (elm, acc) =>
                    {
                        await Task.Yield();
                        return acc + elm;
                    },
                    alias: "Aggregate"
                );
                return result;
            });

        var result = await registration.Invoke(id.Instance, "test");
        result.ShouldBe(6);

        var effectStore = store.EffectsStore;
        var storedEffects = await effectStore.GetEffectResults(registration.MapToStoredId(id.Instance));

        // Should only have the aggregate effect, not the intermediate child effects
        storedEffects.Count.ShouldBe(1);
        storedEffects.Single().Alias.ShouldBe("Aggregate");
    }

    public abstract Task AggregateEachWithSingleElement();
    public async Task AggregateEachWithSingleElement(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var id = TestFlowId.Create();

        using var registry = new FunctionsRegistry(store);
        var registration = registry.RegisterFunc(
            id.Type,
            async Task<int> (string param, Workflow workflow) =>
            {
                var elms = new[] { 10 };
                var result = await elms.CaptureAggregate(
                    seed: 42,
                    handler: async (elm, acc) =>
                    {
                        await Task.Yield();
                        return acc + elm;
                    },
                    alias: "Aggregate"
                );
                return result;
            });

        var result = await registration.Invoke(id.Instance, "test");
        result.ShouldBe(52);
    }

    public abstract Task GetChildrenReturnsAllChildEffectValues();
    public async Task GetChildrenReturnsAllChildEffectValues(Task<IFunctionStore> storeTask)
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

        var parentId = 1.ToEffectId();
        var child1Id = parentId.CreateChild(1);
        var child2Id = parentId.CreateChild(2);
        var child3Id = parentId.CreateChild(3);

        await effectResults.CreateOrGet(child1Id, "first", alias: null, flush: true);
        await effectResults.CreateOrGet(child2Id, "second", alias: null, flush: true);
        await effectResults.CreateOrGet(child3Id, "third", alias: null, flush: true);

        var childIds = await effectResults.GetChildren(parentId);
        var children = new List<string>();
        foreach (var childId in childIds)
        {
            var option = await effectResults.TryGet<string>(childId);
            if (option.HasValue)
                children.Add(option.Value!);
        }

        children.Count.ShouldBe(3);
        children.ShouldContain("first");
        children.ShouldContain("second");
        children.ShouldContain("third");
    }

    public abstract Task GetChildrenReturnsEmptyListWhenNoChildren();
    public async Task GetChildrenReturnsEmptyListWhenNoChildren(Task<IFunctionStore> storeTask)
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

        var parentId = 1.ToEffectId();

        var children = await effectResults.GetChildren(parentId);

        children.Count.ShouldBe(0);
    }

    public abstract Task GetChildrenReturnsAllDescendants();
    public async Task GetChildrenReturnsAllDescendants(Task<IFunctionStore> storeTask)
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

        var parentId = 1.ToEffectId();
        var child1Id = parentId.CreateChild(1);
        var child2Id = parentId.CreateChild(2);
        var grandChildId = child1Id.CreateChild(1);
        var unrelatedId = 2.ToEffectId();

        await effectResults.CreateOrGet(child1Id, 100, alias: null, flush: true);
        await effectResults.CreateOrGet(child2Id, 200, alias: null, flush: true);
        await effectResults.CreateOrGet(grandChildId, 300, alias: null, flush: true);
        await effectResults.CreateOrGet(unrelatedId, 400, alias: null, flush: true);

        var childIds = await effectResults.GetChildren(parentId);
        var children = new List<int>();
        foreach (var childId in childIds)
        {
            var option = await effectResults.TryGet<int>(childId);
            if (option.HasValue)
                children.Add(option.Value);
        }

        children.Count.ShouldBe(3);
        children.ShouldContain(100);
        children.ShouldContain(200);
        children.ShouldContain(300);
        children.ShouldNotContain(400);
    }
}