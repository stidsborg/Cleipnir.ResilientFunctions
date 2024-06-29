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
        var functionTypeId = nameof(PostponedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        {
            var unhandledExceptionHandler = new UnhandledExceptionCatcher();
            var crashableStore = new CrashableFunctionStore(store);
            using var functionsRegistry = new FunctionsRegistry
                (
                    crashableStore,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        leaseLength: TimeSpan.Zero,
                        watchdogCheckFrequency: TimeSpan.Zero
                    )
                );
            var rFunc = functionsRegistry.RegisterFunc<string, string>(
                functionTypeId,
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
                    leaseLength: TimeSpan.Zero,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(2)
                )
            );

            var rFunc = functionsRegistry
                .RegisterFunc(
                    functionTypeId,
                    (string s) => s.ToUpper().ToTask()
                ).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            await rFunc(param, param).ShouldBeAsync("TEST");
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedFuncWithStateIsCompletedByWatchDog();
    protected async Task PostponedFuncWithStateIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(PostponedFuncWithStateIsCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var functionsRegistry = new FunctionsRegistry
                (
                    store,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        watchdogCheckFrequency: TimeSpan.Zero
                    )
                );
            var rFunc = functionsRegistry.RegisterFunc<string, string>(
                    functionTypeId,
                    (_, _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1_000))
                )
                .Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rFunc(param, param));
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
                    functionTypeId,
                    async (string s, Workflow workflow) =>
                    {
                        var state = workflow.States.CreateOrGet<State>("State");
                        state.Value = 1;
                        await state.Save();
                        return s.ToUpper();
                    }
                ).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.Zero,
                    watchdogCheckFrequency: TimeSpan.Zero
                )
            );
            var rAction = functionsRegistry.RegisterAction(
                functionTypeId,
                (string _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1_000))
            ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rAction(functionInstanceId.Value, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.Zero,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );

            var rFunc = functionsRegistry
                .RegisterFunc(
                    functionTypeId,
                    (string s) => s.ToUpper().ToTask()
                ).Invoke;
            
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            await rFunc(functionInstanceId.Value, param);
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionWithStateIsCompletedByWatchDog();
    protected async Task PostponedActionWithStateIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.Zero,
                    watchdogCheckFrequency: TimeSpan.Zero
                )
            );
            var rAction = functionsRegistry.RegisterAction(
                functionTypeId,
                (string _, Workflow _) => Postpone.For(1_000)
            ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => 
                rAction(functionInstanceId.Value, param)
            );
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.Zero,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)
                )
            );
            
            var rFunc = functionsRegistry
                .RegisterAction(
                    functionTypeId,
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

            await rFunc(functionInstanceId.Value, param);
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionIsCompletedByWatchDogAfterCrash();
    protected async Task PostponedActionIsCompletedByWatchDogAfterCrash(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var functionsRegistry = new FunctionsRegistry
            (
                crashableStore,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.Zero,
                    watchdogCheckFrequency: TimeSpan.FromSeconds(10)
                )
            );
            var rFunc = functionsRegistry
                .RegisterAction(functionId.TypeId, (string _) => Postpone.For(1_000))
                .Invoke;

            var instanceId = functionId.InstanceId.ToString();
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
                    leaseLength: TimeSpan.Zero,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            functionsRegistry.RegisterAction(functionId.TypeId, (string _) => { });
            
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
        }
    }

    public abstract Task ThrownPostponeExceptionResultsInPostponedAction();
    protected async Task ThrownPostponeExceptionResultsInPostponedAction(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var functionTypeId = nameof(ThrownPostponeExceptionResultsInPostponedAction);
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            (string _) => Postpone.Throw(postponeFor: TimeSpan.FromSeconds(10))
        );

        //invoke
        {
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.Invoke("invoke", "hello")
            );
            var (status, postponedUntil) = await store
                .GetFunction(new FunctionId(functionTypeId, "invoke"))
                .Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule
        {
            var functionId = new FunctionId(functionTypeId, "schedule");
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
            var functionId = new FunctionId(functionTypeId, "re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();
            
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.ControlPanel(functionId.InstanceId.Value).Result!.ReInvoke()
            );
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FunctionId(functionTypeId, "schedule_re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            await rAction.ControlPanel(functionId.InstanceId).Result!.ScheduleReInvoke();
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ThrownPostponeExceptionResultsInPostponedActionWithState();
    protected async Task ThrownPostponeExceptionResultsInPostponedActionWithState(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var functionTypeId = nameof(ThrownPostponeExceptionResultsInPostponedActionWithState);
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            (string _, Workflow _) => Postpone.Throw(postponeFor: TimeSpan.FromSeconds(10))
        );

        //invoke
        {
            var functionId = new FunctionId(functionTypeId, "invoke");
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.Invoke(functionId.InstanceId.Value, "hello")
            );
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);            
        }
        //schedule
        {
            var functionId = new FunctionId(functionTypeId, "schedule");
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
            var functionId = new FunctionId(functionTypeId, "re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(), 
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();
            
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.ControlPanel(functionId.InstanceId).Result!.ReInvoke()
            );
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FunctionId(functionTypeId, "schedule_re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            await rAction.ControlPanel(functionId.InstanceId).Result!.ScheduleReInvoke();
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ThrownPostponeExceptionResultsInPostponedFunc();
    protected async Task ThrownPostponeExceptionResultsInPostponedFunc(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var functionTypeId = TestFunctionId.Create().TypeId;
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            functionTypeId,
            string (string _) => throw new PostponeInvocationException(TimeSpan.FromSeconds(10))
        );

        //invoke
        {
            Should.Throw<FunctionInvocationPostponedException>(
                () => rFunc.Invoke("invoke", "hello")
            );
            var (status, postponedUntil) = await store
                .GetFunction(new FunctionId(functionTypeId, "invoke"))
                .Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule
        {
            var functionId = new FunctionId(functionTypeId, "schedule");

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
            var functionId = new FunctionId(functionTypeId, "re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();
            var controlPanel = await rFunc.ControlPanel(functionId.InstanceId).ShouldNotBeNullAsync();
            Should.Throw<FunctionInvocationPostponedException>(() => controlPanel.ReInvoke());
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FunctionId(functionTypeId, "schedule_re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanel(functionId.InstanceId).ShouldNotBeNullAsync();
            await controlPanel.ScheduleReInvoke();

            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ThrownPostponeExceptionResultsInPostponedFuncWithState();
    protected async Task ThrownPostponeExceptionResultsInPostponedFuncWithState(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var functionTypeId = TestFunctionId.Create().TypeId;
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rFunc = functionsRegistry.RegisterFunc<string, string>(
            functionTypeId,
            string (string _) => throw new PostponeInvocationException(TimeSpan.FromSeconds(10))
        );

        //invoke
        {
            var functionId = new FunctionId(functionTypeId, "invoke");
            Should.Throw<FunctionInvocationPostponedException>(
                () => rFunc.Invoke(functionId.InstanceId.Value, "hello")
            );
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);            
        }
        //schedule
        {
            var functionId = new FunctionId(functionTypeId, "schedule");

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
            var functionId = new FunctionId(functionTypeId, "re-invoke");
            await store.CreateFunction(
                functionId,
                "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanel(functionId.InstanceId).ShouldNotBeNullAsync();
            Should.Throw<FunctionInvocationPostponedException>(() => controlPanel.ReInvoke());
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FunctionId(functionTypeId, "schedule_re-invoke");
            await store.CreateFunction(
                functionId,
                param: "hello".ToJson(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanel(functionId.InstanceId).ShouldNotBeNullAsync();
            await controlPanel.ScheduleReInvoke();

            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ExistingEligiblePostponedFunctionWillBeReInvokedImmediatelyAfterStartUp();
    protected async Task ExistingEligiblePostponedFunctionWillBeReInvokedImmediatelyAfterStartUp(Task<IFunctionStore> storeTask)
    {
        var functionId = TestFunctionId.Create();
        var (functionTypeId, _) = functionId;
        
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
            functionTypeId,
            void (string param) =>
            {
                syncedParam.Value = param; 
                invokedFlag.Raise();
            });

        await BusyWait.UntilAsync(() => invokedFlag.IsRaised, maxWait: TimeSpan.FromSeconds(10));
        syncedParam.Value.ShouldBe("hello");
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    private class State : Domain.WorkflowState
    {
        public int Value { get; set; }
    }
    
    public abstract Task ScheduleAtActionIsCompletedAfterDelay();
    protected async Task ScheduleAtActionIsCompletedAfterDelay(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var flag = new SyncedFlag();

        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.Zero,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );
        var rAction = functionsRegistry
            .RegisterAction(
                functionId.TypeId,
                (string _) =>
                {
                    flag.Raise();
                });

        await rAction.ScheduleAt(
            functionId.InstanceId.ToString(),
            "param",
            delayUntil: DateTime.UtcNow.AddSeconds(1)
        );

        var sf = await store.GetFunction(functionId).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Postponed);

        var controlPanel = await rAction.ControlPanel(functionId.InstanceId);
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
        var functionId = TestFunctionId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var flag = new SyncedFlag();

        using var functionsRegistry = new FunctionsRegistry
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.Zero,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );
        var rFunc = functionsRegistry
            .RegisterFunc(
                functionId.TypeId,
                (string _) =>
                {
                    flag.Raise();
                    return "ok";
                });

        await rFunc.ScheduleAt(
            functionId.InstanceId.ToString(),
            "param",
            delayUntil: DateTime.UtcNow.AddSeconds(1)
        );

        var sf = await store.GetFunction(functionId).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Postponed);

        var controlPanel = await rFunc.ControlPanel(functionId.InstanceId);
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
        var functionId = TestFunctionId.Create();
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
                functionId.TypeId,
                (string _, Workflow workflow) => workflow.Delay("Delay", TimeSpan.FromDays(1))
            );

        await Should.ThrowAsync<FunctionInvocationPostponedException>(
            () => rFunc.Invoke(functionId.InstanceId.Value, "test")
        );
    }
    
    public abstract Task WorkflowDelayWithDateTimeInvocationDelaysFunction();
    protected async Task WorkflowDelayWithDateTimeInvocationDelaysFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
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
                functionId.TypeId,
                (string _, Workflow workflow) => workflow.Delay("Delay", tomorrow)
            );

        await Should.ThrowAsync<FunctionInvocationPostponedException>(
            () => registration.Invoke(functionId.InstanceId.Value, "test")
        );

        var controlPanel = await registration.ControlPanel(functionId.InstanceId);
        controlPanel.ShouldNotBeNull();

        var delay = controlPanel.Effects.GetValue<DateTime>("Delay");
        delay.ShouldBe(tomorrow);
    }
}