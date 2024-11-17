using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class SunshineTests
{
    public abstract Task SunshineScenarioFunc();
    public async Task SunshineScenarioFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(SunshineScenarioFunc).ToFlowType();
        async Task<string> ToUpper(string s)
        {
            await Task.Delay(10);
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var reg = functionsRegistry
            .RegisterFunc(
                flowType,
                (string s) => ToUpper(s)
            );
        var rFunc = reg.Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");
            
        var storedFunction = await store.GetFunction(
            reg.MapToStoredId(
                new FlowId(
                    flowType, 
                    "hello".ToFlowInstance()
                )
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var storedResult = storedFunction.Result.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>();
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task SunshineScenarioParamless();
    public async Task SunshineScenarioParamless(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = TestFlowId.Create().Type;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var flag = new SyncedFlag();
        var reg = functionsRegistry.RegisterParamless(
            flowType,
            inner: () =>
            {
                flag.Raise();
                return Task.CompletedTask;
            });
        var invoke = reg.Invoke;

        await invoke("SomeInstanceId");
        flag.Position.ShouldBe(FlagPosition.Raised);
            
        var storedFunction = await store.GetFunction(reg.MapToStoredId(new FlowId(flowType, flowInstance: "SomeInstanceId")));
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldBeNull();
        storedFunction.Parameter.ShouldBeNull();
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task SunshineScenarioParamlessWithResultReturnType();
    public async Task SunshineScenarioParamlessWithResultReturnType(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = TestFlowId.Create().Type;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var flag = new SyncedFlag();
        var reg = functionsRegistry
            .RegisterParamless(
                flowType,
                inner: () =>
                {
                    flag.Raise();
                    return Result.Succeed.ToTask();
                });
        var invoke = reg.Invoke;

        await invoke("SomeInstanceId");
        flag.Position.ShouldBe(FlagPosition.Raised);
            
        var storedFunction = await store.GetFunction(reg.MapToStoredId(new FlowId(flowType, flowInstance: "SomeInstanceId")));
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldBeNull();
        storedFunction.Parameter.ShouldBeNull();
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
        
    public abstract Task SunshineScenarioFuncWithState();
    public async Task SunshineScenarioFuncWithState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(SunshineScenarioFuncWithState).ToFlowType();
        async Task<string> ToUpper(string s, State state)
        {
            await state.Save();
            return s.ToUpper();
        }

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var reg = functionsRegistry
            .RegisterFunc(
                flowType,
                async Task<string> (string s, Workflow workflow) =>
                    await ToUpper(s, await workflow.States.CreateOrGet<State>("State"))
            );
        var rFunc = reg.Invoke;

        var result = await rFunc("hello", "hello");
        result.ShouldBe("HELLO");

        var storedFunction = await store.GetFunction(
            reg.MapToStoredId(new FlowId(
                    flowType,
                    "hello".ToFlowInstance()
                )
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldNotBeNull();
        var storedResult = storedFunction.Result.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>();
        storedResult.ShouldBe("HELLO");
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
        
    public abstract Task SunshineScenarioAction();
    public async Task SunshineScenarioAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(SunshineScenarioAction).ToFlowType();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var reg = functionsRegistry
            .RegisterAction(
                flowType,
                (string _) => Task.Delay(10)
            );
        var rAction = reg.Invoke;

        await rAction("hello", "hello");

        var storedFunction = await store.GetFunction(
            reg.MapToStoredId(new FlowId(
                    flowType,
                    "hello".ToFlowInstance()
                )
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
        
    public abstract Task SunshineScenarioActionWithState();
    public async Task SunshineScenarioActionWithState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(SunshineScenarioActionWithState).ToFlowType();

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var reg = functionsRegistry
            .RegisterAction(
                flowType,
                async (string s, Workflow workflow) => await (await workflow.States.CreateOrGet<State>("State")).Save()
            );
        var rFunc = reg.Invoke;

        await rFunc("hello", "hello");

        var storedFunction = await store.GetFunction(
            reg.MapToStoredId(
                new FlowId(
                    flowType,
                    "hello".ToFlowInstance()
                )
            )
        );
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }

    public abstract Task SunshineScenarioNullReturningFunc();
    protected async Task SunshineScenarioNullReturningFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FlowType flowType = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            (string s) => default(string).ToTask()
        ).Invoke;

        var result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();
    }

    public abstract Task SunshineScenarioNullReturningFuncWithState();
    protected async Task SunshineScenarioNullReturningFuncWithState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var reg = functionsRegistry.RegisterFunc(
            flowType,
            async (string _, Workflow workflow) =>
            {
                var state = await workflow.States.CreateOrGet<ListState<string>>("State");
                state.List.Add("hello world");
                state.Save().Wait();
                return default(string);
            }
        );
        var rFunc = reg.Invoke;

        var result = await rFunc(flowInstance.Value, "hello world");
        result.ShouldBeNull();

        await store
            .GetFunction(reg.MapToStoredId(functionId))
            .ShouldNotBeNullAsync();

        var states = await store.EffectsStore.GetEffectResults(reg.MapToStoredId(functionId));
        var state = states.Single(e => e.EffectId == "State").Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<ListState<string>>();
        state.List.Single().ShouldBe("hello world");
    }
    
    public abstract Task SecondInvocationOnNullReturningFuncReturnsNullSuccessfully();
    protected async Task SecondInvocationOnNullReturningFuncReturnsNullSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        FlowType flowType = "SomeFunctionType";
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            async (string _, Workflow workflow) =>
            {
                var state = await workflow.States.CreateOrGet<ListState<string>>("State");
                state.List.Add("hello world");
                state.Save().Wait();
                return default(string);
            }
        ).Invoke;

        var result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();

        result = await rFunc("hello world", "hello world");
        result.ShouldBeNull();
    }
    
    public abstract Task FunctionIsRemovedAfterRetentionPeriod();
    protected async Task FunctionIsRemovedAfterRetentionPeriod(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var functionId = TestFlowId.Create();
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    enableWatchdogs: false
                )
            );
        
            var rFunc = functionsRegistry.RegisterAction(
                functionId.Type,
                inner: (string _) => Task.CompletedTask
            ).Invoke;

            await rFunc("hello world", "hello world");
        }

        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    enableWatchdogs: true,
                    retentionPeriod: TimeSpan.Zero
                )
            );

            var reg = functionsRegistry.RegisterAction(
                functionId.Type,
                inner: (string _) => Task.CompletedTask
            );
            
            await BusyWait.Until(async () => await store.GetFunction(reg.MapToStoredId(functionId)) is null);
        }
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    private class State : Domain.FlowState {}
    
    public abstract Task InstancesCanBeFetched();
    protected async Task InstancesCanBeFetched(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var flowType = TestFlowId.Create().Type;
        
        
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    enableWatchdogs: false
                )
            );
        
            var registration = functionsRegistry.RegisterAction(
                flowType,
                inner: Task<Result> (bool postpone) => 
                    Task.FromResult(
                        postpone 
                        ? Postpone.For(TimeSpan.FromHours(1)) 
                        : Succeed.WithoutValue
                    )
            );
            
            await registration.Schedule("true", true);
            await registration.Schedule("false", false);

            var trueFlowControlPanel = (await registration.ControlPanel("true")).ShouldNotBeNull();
            await BusyWait.Until(async () =>
            {
                await trueFlowControlPanel.Refresh();
                return trueFlowControlPanel.Status == Status.Postponed;
            });
            
            var falseFlowControlPanel = (await registration.ControlPanel("false")).ShouldNotBeNull();
            await falseFlowControlPanel.WaitForCompletion();

            var allInstances = await registration.GetInstances();
            allInstances.Count.ShouldBe(2);
            allInstances.Any(i => i == "true").ShouldBeTrue();
            allInstances.Any(i => i == "false").ShouldBeTrue();

            var succeeds = await registration.GetInstances(Status.Succeeded);
            succeeds.Count.ShouldBe(1);
            succeeds.Single().ShouldBe("false");
            
            var postponed = await registration.GetInstances(Status.Postponed);
            postponed.Count.ShouldBe(1);
            postponed.Single().ShouldBe("true");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task EffectsAreNotFetchedOnFirstInvocation();
    protected async Task EffectsAreNotFetchedOnFirstInvocation(Task<IFunctionStore> storeTask)
    {
        var store = new CrashableFunctionStore(await storeTask);

        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var functionId = TestFlowId.Create();

        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                enableWatchdogs: false
            )
        );

        var invoke = functionsRegistry.RegisterParamless(
            functionId.Type,
            inner: async workflow =>
            {
                store.Crash();
                await workflow.Effect.Contains("test");
                store.FixCrash();
            }
        ).Invoke;

        await invoke("test");
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task EffectsAreFetchedOnSecondInvocation();
    protected async Task EffectsAreFetchedOnSecondInvocation(Task<IFunctionStore> storeTask)
    {
        var store = new CrashableFunctionStore(await storeTask);
        var functionId = TestFlowId.Create();

        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                enableWatchdogs: true,
                watchdogCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );

        var flag = new SyncedFlag();
        var registration = functionsRegistry.RegisterParamless(
            functionId.Type,
            inner: async workflow =>
            {
                store.Crash();
                try
                {
                    await workflow.Effect.Contains("test");
                }
                catch
                {
                    store.FixCrash();
                    flag.Raise();
                    throw;
                }
            }
        );
        
        await registration.ScheduleIn("test", TimeSpan.FromSeconds(1));
        await flag.WaitForRaised();
        
        var controlPanel = await registration.ControlPanel("test").ShouldNotBeNullAsync();
        await BusyWait.Until(
            async () =>
            {
                await controlPanel.Refresh();
                return controlPanel.Status == Status.Failed || controlPanel.Status == Status.Succeeded;
            }
        );
        
        controlPanel.Status.ShouldBe(Status.Failed);
    }
    
    public abstract Task FlowIdCanBeExtractedFromWorkflowInstance();
    public async Task FlowIdCanBeExtractedFromWorkflowInstance(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(SunshineScenarioFunc).ToFlowType();
        var flowInstance = Guid.NewGuid().ToString();
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        FlowId? flowId = null;
        var reg = functionsRegistry
            .RegisterParamless(
                flowType,
                workflow =>
                {
                    flowId = workflow.FlowId;
                    return Task.CompletedTask;
                }
            );
        await reg.Invoke(flowInstance);
            
        flowId.ShouldBe(new FlowId(flowType, flowInstance));
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task FlowIdCanBeExtractedFromAmbientState();
    public async Task FlowIdCanBeExtractedFromAmbientState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var (type, instance) = TestFlowId.Create();
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        FlowId? flowId = null;
        var reg = functionsRegistry
            .RegisterParamless(
                type,
                inner: () =>
                {
                    flowId = CurrentFlow.Id;
                    return Task.CompletedTask;
                }
            );
        await reg.Invoke(instance);
            
        flowId.ShouldBe(new FlowId(type, instance));
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task FlowIdCanBeExtractedFromAmbientStateAfterSuspension();
    public async Task FlowIdCanBeExtractedFromAmbientStateAfterSuspension(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var (type, instance) = TestFlowId.Create();
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        var postponed = false;
        
        FlowId? flowId = null;
        var reg = functionsRegistry
            .RegisterParamless(
                type,
                inner: () =>
                {
                    if (!postponed)
                    {
                        postponed = true;
                        return Postpone.For(100).ToResult().ToTask();
                    }
                        
                    flowId = CurrentFlow.Id;
                    return Result.Succeed.ToTask();
                }
            );
        await reg.Schedule(instance);

        await BusyWait.Until(() => flowId != null);
        
        flowId.ShouldBe(new FlowId(type, instance));
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}