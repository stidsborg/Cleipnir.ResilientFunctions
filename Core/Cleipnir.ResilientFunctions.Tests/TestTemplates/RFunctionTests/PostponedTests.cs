using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class PostponedTests
{
    public abstract Task PostponedFuncIsCompletedByWatchDog();
    protected async Task PostponedFuncIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(PostponedFuncIsCompletedByWatchDog).ToFlowType();
        const string param = "test";
        {
            var unhandledExceptionHandler = new UnhandledExceptionCatcher();
            var crashableStore = new CrashableFunctionStore(store);
            using var functionsRegistry = new FunctionsRegistry
                (
                    crashableStore,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        enableWatchdogs: false
                    )
                );
            var rFunc = functionsRegistry.RegisterFunc<string, string>(
                flowType,
                (string _) => Postpone.For(1_000)
            ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() =>
                rFunc(param, param)
            );
            crashableStore.Crash();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            var unhandledExceptionHandler = new UnhandledExceptionCatcher();
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );

            var rFunc = functionsRegistry
                .RegisterFunc(
                    flowType,
                    (string s) => s.ToUpper().ToTask()
                ).Invoke;

            var functionId = new FlowId(flowType, param.ToFlowInstance());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            await rFunc(param, param).ShouldBeAsync("TEST");
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedFuncWithStateIsCompletedByWatchDog();
    protected async Task PostponedFuncWithStateIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(PostponedFuncWithStateIsCompletedByWatchDog).ToFlowType();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var functionsRegistry = new FunctionsRegistry
                (
                    store,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        enableWatchdogs: false
                    )
                );
            var rFunc = functionsRegistry
                .RegisterFunc<string, string>(
                    flowType,
                    inner: (_, _) => throw new PostponeInvocationException(1_000)
                ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rFunc(param, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(1000)
                )
            );

            var rFunc = functionsRegistry
                .RegisterFunc(
                    flowType,
                    async (string s, Workflow workflow) =>
                    {
                        var state = workflow.States.CreateOrGet<State>("State");
                        state.Value = 1;
                        await state.Save();
                        return s.ToUpper();
                    }
                ).Invoke;

            var functionId = new FlowId(flowType, param.ToFlowInstance());

            try
            {
                await BusyWait.Until(
                    async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded,
                    maxWait: TimeSpan.FromSeconds(10)
                );
            }
            catch (TimeoutException)
            {
                unhandledExceptionHandler.ShouldNotHaveExceptions();
                var sf = await store.GetFunction(functionId); 
                throw new TimeoutException(
                    "Timeout when waiting for function completion - has status: " 
                    + sf!.Status +
                    " and expires: " + sf.PostponedUntil + 
                    " ticks now is: " + DateTime.UtcNow.Ticks
                );
            }
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();

            var states = await store.StatesStore.GetStates(functionId);
            var state = states.Single(e => e.StateId == "State");
            state.StateJson.DeserializeFromJsonTo<State>().Value.ShouldBe(1);
            
            await rFunc(param, param).ShouldBeAsync("TEST");
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionIsCompletedByWatchDog();
    protected async Task PostponedActionIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.Zero,
                    enableWatchdogs: false
                )
            );
            var rAction = functionsRegistry.RegisterAction(
                flowType,
                (string _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1_000))
            ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rAction(flowInstance.Value, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );

            var rFunc = functionsRegistry
                .RegisterFunc(
                    flowType,
                    (string s) => s.ToUpper().ToTask()
                ).Invoke;
            
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            await rFunc(flowInstance.Value, param);
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionWithStateIsCompletedByWatchDog();
    protected async Task PostponedActionWithStateIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    enableWatchdogs: false
                )
            );
            var rAction = functionsRegistry.RegisterAction(
                flowType,
                (string _, Workflow _) => Postpone.For(1_000)
            ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => 
                rAction(flowInstance.Value, param)
            );
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(250)
                )
            );
            
            var rFunc = functionsRegistry
                .RegisterAction(
                    flowType,
                    async (string _, Workflow workflow) =>
                    {
                        var state = workflow.States.CreateOrGet<State>("State");
                        state.Value = 1;
                        await state.Save();
                    }
                ).Invoke;
            
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();

            var states = await store.StatesStore.GetStates(functionId);
            var state = states.Single(e => e.StateId == "State");
            state.StateJson.DeserializeFromJsonTo<State>().Value.ShouldBe(1);

            await rFunc(flowInstance.Value, param);
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionIsCompletedByWatchDogAfterCrash();
    protected async Task PostponedActionIsCompletedByWatchDogAfterCrash(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var functionsRegistry = new FunctionsRegistry
            (
                crashableStore,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.Zero,
                    enableWatchdogs: false
                )
            );
            var rFunc = functionsRegistry
                .RegisterAction(functionId.Type, (string _) => Postpone.For(1_000))
                .Invoke;

            var instanceId = functionId.Instance.ToString();
            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => _ = rFunc(instanceId, "param"));
            crashableStore.Crash();
        }
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var functionsRegistry = new FunctionsRegistry
            (
                crashableStore,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            functionsRegistry.RegisterAction(functionId.Type, (string _) => { });
            
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
        }
    }

    public abstract Task ThrownPostponeExceptionResultsInPostponedAction();
    protected async Task ThrownPostponeExceptionResultsInPostponedAction(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var flowType = nameof(ThrownPostponeExceptionResultsInPostponedAction);
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            (string _) => Postpone.Throw(postponeFor: TimeSpan.FromSeconds(10))
        );

        //invoke
        {
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.Invoke("invoke", "hello")
            );
            var (status, postponedUntil) = await store
                .GetFunction(new FlowId(flowType, "invoke"))
                .Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule
        {
            var functionId = new FlowId(flowType, "schedule");
            await rAction.Schedule("schedule", "hello");
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store
                .GetFunction(functionId)
                .Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //re-invoke
        {
            var functionId = new FlowId(flowType, "re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();
            
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.ControlPanel(functionId.Instance.Value).Result!.Restart()
            );
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FlowId(flowType, "schedule_re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            await rAction.ControlPanel(functionId.Instance).Result!.ScheduleRestart();
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ThrownPostponeExceptionResultsInPostponedActionWithState();
    protected async Task ThrownPostponeExceptionResultsInPostponedActionWithState(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var flowType = nameof(ThrownPostponeExceptionResultsInPostponedActionWithState);
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            (string _, Workflow _) => Postpone.Throw(postponeFor: TimeSpan.FromSeconds(10))
        );

        //invoke
        {
            var functionId = new FlowId(flowType, "invoke");
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.Invoke(functionId.Instance.Value, "hello")
            );
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);            
        }
        //schedule
        {
            var functionId = new FlowId(flowType, "schedule");
            await rAction.Schedule("schedule", "hello");
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store
                .GetFunction(functionId)
                .Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //re-invoke
        {
            var functionId = new FlowId(flowType, "re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(), 
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();
            
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.ControlPanel(functionId.Instance).Result!.Restart()
            );
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FlowId(flowType, "schedule_re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            await rAction.ControlPanel(functionId.Instance).Result!.ScheduleRestart();
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }

    public abstract Task ThrownPostponeExceptionResultsInPostponedFunc();
    protected async Task ThrownPostponeExceptionResultsInPostponedFunc(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var flowType = TestFlowId.Create().Type;
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            Task<string> (string _) => throw new PostponeInvocationException(TimeSpan.FromSeconds(10))
        );

        //invoke
        {
            Should.Throw<FunctionInvocationPostponedException>(
                () => rFunc.Invoke("invoke", "hello")
            );
            var (status, postponedUntil) = await store
                .GetFunction(new FlowId(flowType, "invoke"))
                .Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule
        {
            var functionId = new FlowId(flowType, "schedule");

            await rFunc.Schedule("schedule", "hello");
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            var (status, postponedUntil) = await store
                .GetFunction(functionId)
                .Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //re-invoke
        {
            var functionId = new FlowId(flowType, "re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();
            var controlPanel = await rFunc.ControlPanel(functionId.Instance).ShouldNotBeNullAsync();
            Should.Throw<FunctionInvocationPostponedException>(() => controlPanel.Restart());
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FlowId(flowType, "schedule_re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanel(functionId.Instance).ShouldNotBeNullAsync();
            await controlPanel.ScheduleRestart();

            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ThrownPostponeExceptionResultsInPostponedFuncWithState();
    protected async Task ThrownPostponeExceptionResultsInPostponedFuncWithState(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var flowType = TestFlowId.Create().Type;
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            Task<string> (string _) => throw new PostponeInvocationException(TimeSpan.FromSeconds(10))
        );

        //invoke
        {
            var functionId = new FlowId(flowType, "invoke");
            Should.Throw<FunctionInvocationPostponedException>(
                () => rFunc.Invoke(functionId.Instance.Value, "hello")
            );
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);            
        }
        //schedule
        {
            var functionId = new FlowId(flowType, "schedule");

            await rFunc.Schedule("schedule", "hello");
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            var (status, postponedUntil) = await store
                .GetFunction(functionId)
                .Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //re-invoke
        {
            var functionId = new FlowId(flowType, "re-invoke");
            await store.CreateFunction(
                functionId,
                "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanel(functionId.Instance).ShouldNotBeNullAsync();
            Should.Throw<FunctionInvocationPostponedException>(() => controlPanel.Restart());
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FlowId(flowType, "schedule_re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanel(functionId.Instance).ShouldNotBeNullAsync();
            await controlPanel.ScheduleRestart();

            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }

        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    public abstract Task ExistingEligiblePostponedFunctionWillBeReInvokedImmediatelyAfterStartUp();
    protected async Task ExistingEligiblePostponedFunctionWillBeReInvokedImmediatelyAfterStartUp(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFlowId.Create();
        var (flowType, _) = functionId;
        
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;

        var storedParameter = "hello".ToJson();
        
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.AddDays(-1).Ticks,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToFunc(), LeaseLength: 0)
        ).ShouldBeTrueAsync();

        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: unhandledExceptionCatcher.Catch,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)
            )
        );

        var syncedParam = new Synced<string>();
        var invokedFlag = new SyncedFlag();
        functionsRegistry.RegisterAction(
            flowType,
            void (string param) =>
            {
                syncedParam.Value = param; 
                invokedFlag.Raise();
            });

        await BusyWait.UntilAsync(() => invokedFlag.IsRaised, maxWait: TimeSpan.FromSeconds(10));
        syncedParam.Value.ShouldBe("hello");
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    private class State : Domain.FlowState
    {
        public int Value { get; set; }
    }
    
    public abstract Task ScheduleAtActionIsCompletedAfterDelay();
    protected async Task ScheduleAtActionIsCompletedAfterDelay(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var flag = new SyncedFlag();

        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                watchdogCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        var rAction = functionsRegistry
            .RegisterAction(
                functionId.Type,
                (string _) =>
                {
                    flag.Raise();
                });

        await rAction.ScheduleAt(
            functionId.Instance.ToString(),
            "param",
            delayUntil: DateTime.UtcNow.AddSeconds(1)
        );

        var sf = await store.GetFunction(functionId).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Postponed);

        var controlPanel = await rAction.ControlPanel(functionId.Instance);
        controlPanel.ShouldNotBeNull();
        await BusyWait.Until(async () =>
            {
                await controlPanel.Refresh();
                return controlPanel.Status == Status.Succeeded;
            });

        flag.IsRaised.ShouldBeTrue();
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ScheduleAtFuncIsCompletedAfterDelay();
    protected async Task ScheduleAtFuncIsCompletedAfterDelay(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var flag = new SyncedFlag();

        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.Zero,
                watchdogCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        var rFunc = functionsRegistry
            .RegisterFunc(
                functionId.Type,
                (string _) =>
                {
                    flag.Raise();
                    return "ok".ToTask();
                });

        await rFunc.ScheduleAt(
            functionId.Instance.ToString(),
            "param",
            delayUntil: DateTime.UtcNow.AddSeconds(1)
        );

        var sf = await store.GetFunction(functionId).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Postponed);

        var controlPanel = await rFunc.ControlPanel(functionId.Instance);
        controlPanel.ShouldNotBeNull();
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });
        flag.IsRaised.ShouldBeTrue();
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task WorkflowDelayInvocationDelaysFunction();
    protected async Task WorkflowDelayInvocationDelaysFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );
        var rFunc = functionsRegistry
            .RegisterAction(
                functionId.Type,
                (string _, Workflow workflow) => workflow.Delay("Delay", TimeSpan.FromDays(1))
            );

        await Should.ThrowAsync<FunctionInvocationPostponedException>(
            () => rFunc.Invoke(functionId.Instance.Value, "test")
        );
    }
    
    public abstract Task WorkflowDelayWithDateTimeInvocationDelaysFunction();
    protected async Task WorkflowDelayWithDateTimeInvocationDelaysFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();

        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(250)
            )
        );
        var tomorrow = DateTime.UtcNow.Add(TimeSpan.FromDays(1));
        var registration = functionsRegistry
            .RegisterAction(
                functionId.Type,
                (string _, Workflow workflow) => workflow.Delay("Delay", tomorrow)
            );

        await Should.ThrowAsync<FunctionInvocationPostponedException>(
            () => registration.Invoke(functionId.Instance.Value, "test")
        );

        var controlPanel = await registration.ControlPanel(functionId.Instance);
        controlPanel.ShouldNotBeNull();

        var delay = controlPanel.Effects.GetValue<DateTime>("Delay");
        delay.ShouldBe(tomorrow);
    }
}