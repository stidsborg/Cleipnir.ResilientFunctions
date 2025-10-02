using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

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
            reg.MapToStoredId("hello".ToFlowInstance())
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
            
        var storedFunction = await store.GetFunction(reg.MapToStoredId("SomeInstanceId"));
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
                    return Succeed.WithUnit.ToTask();
                });
        var invoke = reg.Invoke;

        await invoke("SomeInstanceId");
        flag.Position.ShouldBe(FlagPosition.Raised);
            
        var storedFunction = await store.GetFunction(reg.MapToStoredId("SomeInstanceId"));
        storedFunction.ShouldNotBeNull();
        storedFunction.Status.ShouldBe(Status.Succeeded);
        storedFunction.Result.ShouldBeNull();
        storedFunction.Parameter.ShouldBeNull();
            
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
            reg.MapToStoredId("hello".ToFlowInstance())
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
            
            await BusyWait.Until(async () => await store.GetFunction(reg.MapToStoredId(functionId.Instance)) is null);
        }
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
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
                inner: Task<Result<Unit>> (bool postpone) => 
                    Task.FromResult(
                        postpone 
                        ? Postpone.Until(DateTime.UtcNow.AddHours(1)) 
                        : Succeed.WithUnit
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
            allInstances.Any(i => i == "true".ToStoredId(registration.StoredType)).ShouldBeTrue();
            allInstances.Any(i => i == "false".ToStoredId(registration.StoredType)).ShouldBeTrue();

            var succeeds = await registration.GetInstances(Status.Succeeded);
            succeeds.Count.ShouldBe(1);
            succeeds.Single().ShouldBe("false".ToStoredId(registration.StoredType));
            
            var postponed = await registration.GetInstances(Status.Postponed);
            postponed.Count.ShouldBe(1);
            postponed.Single().ShouldBe("true".ToStoredId(registration.StoredType));
        
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

        StoredId? storedId = null;
        var reg = functionsRegistry
            .RegisterParamless(
                type,
                inner: () =>
                {
                    storedId = CurrentFlow.StoredId;
                    return Task.CompletedTask;
                }
            );
        await reg.Invoke(instance);
        
            
        storedId.ShouldBe(reg.MapToStoredId(instance));
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
        
        StoredId? storedId = null;
        var reg = functionsRegistry
            .RegisterParamless(
                type,
                inner: () =>
                {
                    if (!postponed)
                    {
                        postponed = true;
                        return Postpone.Until(DateTime.UtcNow.AddMilliseconds(100)).ToUnitResult.ToTask();
                    }
                        
                    storedId = CurrentFlow.StoredId;
                    return Succeed.WithUnit.ToTask();
                }
            );
        await reg.Schedule(instance);

        await BusyWait.Until(() => storedId != null);
        
        storedId.ShouldBe(reg.MapToStoredId(instance));
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task FuncCanBeCreatedWithInitialState();
    public async Task FuncCanBeCreatedWithInitialState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create(); 

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        string? initialEffectValue = null;
        string? initialMessageValue = null;
        
        var registration = functionsRegistry
            .RegisterFunc(
                flowId.Type,
                async (string s, Workflow workflow) =>
                {
                    initialEffectValue = await workflow.Effect.Get<string>("InitialEffectId");
                    initialMessageValue = await workflow.Messages.OfType<string>().First();
                    return s;
                });


        await registration.Invoke(
            flowInstance: "hello",
            param: "hello",
            initialState: new InitialState(
                Messages: [new MessageAndIdempotencyKey("InitialMessage")],
                Effects: [new InitialEffect(Id: "InitialEffectId", Value: "InitialEffectValue", Exception: null)]
            )
        );
        
        initialEffectValue.ShouldBe("InitialEffectValue");
        initialMessageValue.ShouldBe("InitialMessage");
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ActionCanBeCreatedWithInitialState();
    public async Task ActionCanBeCreatedWithInitialState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create(); 

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        string? initialEffectValue = null;
        string? initialMessageValue = null;
        
        var registration = functionsRegistry
            .RegisterAction(
                flowId.Type,
                async (string _, Workflow workflow) =>
                {
                    initialEffectValue = await workflow.Effect.Get<string>("InitialEffectId");
                    initialMessageValue = await workflow.Messages.OfType<string>().First();
                });


        await registration.Invoke(
            flowInstance: "hello",
            param: "hello",
            initialState: new InitialState(
                Messages: [new MessageAndIdempotencyKey("InitialMessage")],
                Effects: [new InitialEffect(Id: "InitialEffectId", Value: "InitialEffectValue", Exception: null)]
            )
        );
        
        initialEffectValue.ShouldBe("InitialEffectValue");
        initialMessageValue.ShouldBe("InitialMessage");
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ParamlessCanBeCreatedWithInitialState();
    public async Task ParamlessCanBeCreatedWithInitialState(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create(); 

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        string? initialEffectValue = null;
        string? initialMessageValue = null;
        
        var registration = functionsRegistry
            .RegisterParamless(
                flowId.Type,
                async (Workflow workflow) =>
                {
                    initialEffectValue = await workflow.Effect.Get<string>("InitialEffectId");
                    initialMessageValue = await workflow.Messages.OfType<string>().First();
                });


        await registration.Invoke(
            flowInstance: "hello",
            initialState: new InitialState(
                Messages: [new MessageAndIdempotencyKey("InitialMessage")],
                Effects: [new InitialEffect(Id: "InitialEffectId", Value: "InitialEffectValue", Exception: null)]
            )
        );
        
        initialEffectValue.ShouldBe("InitialEffectValue");
        initialMessageValue.ShouldBe("InitialMessage");
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ParamlessCanBeCreatedWithInitialStateContainedStartedButNotCompletedEffect();
    public async Task ParamlessCanBeCreatedWithInitialStateContainedStartedButNotCompletedEffect(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create(); 

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));

        WorkStatus? workStatus = null;

        var registration = functionsRegistry
            .RegisterParamless(
                flowId.Type,
                async workflow =>
                {
                    workStatus = await workflow.Effect.GetStatus("InitialEffectId");
                }
            );


        await registration.Invoke(
            flowInstance: "hello",
            initialState: new InitialState(
                Messages: [],
                Effects: [new InitialEffect(Id: "InitialEffectId", Status: WorkStatus.Started)]
            )
        );
        
        workStatus.ShouldBe(WorkStatus.Started);
            
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ParamlessCanBeCreatedWithInitialFailedEffect();
    public async Task ParamlessCanBeCreatedWithInitialFailedEffect(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create(); 

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        
        var registration = functionsRegistry
            .RegisterParamless(
                flowId.Type,
                async workflow =>
                {
                    await workflow.Effect.Capture("InitialEffectId", () => Task.CompletedTask);
                }
            );


        try
        {
            await registration.Invoke(
                flowInstance: "hello",
                initialState: new InitialState(
                    Messages: [],
                    Effects: [new InitialEffect(Id: "InitialEffectId", Exception: new TimeoutException())]
                )
            );
            Assert.Fail("Expected TimeoutException");
        }
        catch (FatalWorkflowException exception)
        {
            exception.ErrorType.ShouldBe(typeof(TimeoutException));
        }

        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task FunctionCanAcceptAndReturnOptionType();
    public async Task FunctionCanAcceptAndReturnOptionType(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create(); 

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        var registration = functionsRegistry
            .RegisterFunc(
                flowId.Type,
                Task<Option<string>> (Option<string> param) => param.ToTask()
            );

        {
            var optionWithValue = Option.Create("Hallo World");
            var returnedOption = await registration.Invoke("WithValue", optionWithValue);    
            returnedOption.HasValue.ShouldBeTrue();
            returnedOption.Value.ShouldBe("Hallo World");
        }

        {
            var optionWithoutValue = Option.CreateNoValue<string>();
            var returnedOption = await registration.Invoke("WithoutValue", optionWithoutValue);    
            returnedOption.HasValue.ShouldBeFalse();
        }
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExecutingFunctionHasOwner();
    public async Task ExecutingFunctionHasOwner(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowId = TestFlowId.Create(); 

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler.Catch));
        
        var insideFlag = new SyncedFlag();
        var completeFlag = new SyncedFlag();
        var registration = functionsRegistry
            .RegisterAction(
                flowId.Type,
                inner: async Task (string _) =>
                {
                    insideFlag.Raise();
                    await completeFlag.WaitForRaised();
                }
            );
        var flowTask = registration.Invoke(flowId.Instance, "param");
        await insideFlag.WaitForRaised();

        var cp = await registration.ControlPanel(flowId.Instance).ShouldNotBeNullAsync();
        cp.OwnerReplica.ShouldNotBeNull();
        cp.OwnerReplica.ShouldBe(functionsRegistry.ClusterInfo.ReplicaId);
        
        completeFlag.Raise();
        await flowTask;

        await cp.Refresh();
        //todo cp.Owner.ShouldBeNull();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}