using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
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
                (string _) => Postpone.For(1_000).ToResult<string>().ToTask()
            ).Invoke;

            await Should.ThrowAsync<InvocationPostponedException>(() =>
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
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(1_000)
                )
            );

            var rFunc = functionsRegistry
                .RegisterFunc(
                    flowType,
                    (string s) => s.ToUpper().ToTask()
                );

            var functionId = new FlowId(flowType, param.ToFlowInstance());
            await BusyWait.Until(async () => (await store.GetFunction(rFunc.MapToStoredId(functionId)))!.Status == Status.Succeeded);
            await rFunc.Invoke(param, param).ShouldBeAsync("TEST");
            unhandledExceptionHandler.ShouldNotHaveExceptions();
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

            await Should.ThrowAsync<InvocationPostponedException>(() => rFunc(param, param));
            unhandledExceptionHandler.ShouldNotHaveExceptions();
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
                        var state = await workflow.States.CreateOrGet<State>("State");
                        state.Value = 1;
                        await state.Save();
                        return s.ToUpper();
                    }
                );

            var functionId = new FlowId(flowType, param.ToFlowInstance());

            try
            {
                await BusyWait.Until(
                    async () => (await store.GetFunction(rFunc.MapToStoredId(functionId)))!.Status == Status.Succeeded,
                    maxWait: TimeSpan.FromSeconds(10)
                );
            }
            catch (TimeoutException)
            {
                unhandledExceptionHandler.ShouldNotHaveExceptions();
                var sf = await store.GetFunction(rFunc.MapToStoredId(functionId)); 
                throw new TimeoutException(
                    "Timeout when waiting for function completion - has status: " 
                    + sf!.Status +
                    " and expires: " + sf.Expires + 
                    " ticks now is: " + DateTime.UtcNow.Ticks
                );
            }
            
            var storedFunction = await store.GetFunction(rFunc.MapToStoredId(functionId));
            storedFunction.ShouldNotBeNull();

            var states = await store.EffectsStore.GetEffectResults(rFunc.MapToStoredId(functionId));
            var state = states.Single(e => e.EffectId == "State".ToEffectId(EffectType.State));
            state.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<State>().Value.ShouldBe(1);
            
            await rFunc.Invoke(param, param).ShouldBeAsync("TEST");
            unhandledExceptionHandler.ShouldNotHaveExceptions();
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
                Task<Result<Unit>> (string _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1_000)).ToUnitResult.ToTask()
            ).Invoke;

            await Should.ThrowAsync<InvocationPostponedException>(() => rAction(flowInstance.Value, param));
            unhandledExceptionHandler.ShouldNotHaveExceptions();
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(1_000)
                )
            );

            var rFunc = functionsRegistry
                .RegisterFunc(
                    flowType,
                    (string s) => s.ToUpper().ToTask()
                );
            
            await BusyWait.Until(async () => (await store.GetFunction(rFunc.MapToStoredId(functionId)))!.Status == Status.Succeeded);
            await rFunc.Invoke(flowInstance.Value, param);
            unhandledExceptionHandler.ShouldNotHaveExceptions();
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
                Task<Result<Unit>> (string _, Workflow _) => Postpone.For(1_000).ToUnitResult.ToTask()
            ).Invoke;

            await Should.ThrowAsync<InvocationPostponedException>(() => 
                rAction(flowInstance.Value, param)
            );
            unhandledExceptionHandler.ShouldNotHaveExceptions();
        }
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(1_000)
                )
            );
            
            var rFunc = functionsRegistry
                .RegisterAction(
                    flowType,
                    async (string _, Workflow workflow) =>
                    {
                        var state = await workflow.States.CreateOrGet<State>("State");
                        state.Value = 1;
                        await state.Save();
                    }
                );
            
            await BusyWait.Until(async () => (await store.GetFunction(rFunc.MapToStoredId(functionId)))!.Status == Status.Succeeded);
            var storedFunction = await store.GetFunction(rFunc.MapToStoredId(functionId));
            storedFunction.ShouldNotBeNull();

            var states = await store.EffectsStore.GetEffectResults(rFunc.MapToStoredId(functionId));
            var state = states.Single(e => e.EffectId == "State".ToEffectId(EffectType.State));
            state.Result!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<State>().Value.ShouldBe(1);

            await rFunc.Invoke(flowInstance.Value, param);
            unhandledExceptionHandler.ShouldNotHaveExceptions();
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
                .RegisterAction(functionId.Type, Task<Result<Unit>> (string _) => Postpone.For(1_000).ToUnitResult.ToTask())
                .Invoke;

            var instanceId = functionId.Instance.ToString();
            await Should.ThrowAsync<InvocationPostponedException>(() => _ = rFunc(instanceId, "param"));
            crashableStore.Crash();
        }
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var functionsRegistry = new FunctionsRegistry
            (
                crashableStore,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(1_000)
                )
            );
            var registration = functionsRegistry.RegisterAction(functionId.Type, (string _) => Task.CompletedTask);
            
            await BusyWait.Until(() => store.GetFunction(registration.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Succeeded));
            unhandledExceptionHandler.ShouldNotHaveExceptions();
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
            flowType, Task (string _) =>
            {
                Postpone.Throw(postponeFor: TimeSpan.FromSeconds(10));
                return Task.CompletedTask;
            }
        );

        //invoke
        {
            Should.Throw<InvocationPostponedException>(
                () => rAction.Invoke("invoke", "hello")
            );
            var (status, postponedUntil) = await store
                .GetFunction(rAction.MapToStoredId(new FlowId(flowType, "invoke")))
                .Map(sf => Tuple.Create(sf?.Status, sf?.Expires));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule
        {
            var functionId = new FlowId(flowType, "schedule");
            await rAction.Schedule("schedule", "hello");
            await BusyWait.Until(() => store.GetFunction(rAction.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store
                .GetFunction(rAction.MapToStoredId(functionId))
                .Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //re-invoke
        {
            var functionId = new FlowId(flowType, "re-invoke");
            await store.CreateFunction(
                rAction.MapToStoredId(functionId), 
                "humanInstanceId",
                param: "hello".ToJson().ToUtf8Bytes(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null
            ).ShouldBeTrueAsync();
            
            Should.Throw<InvocationPostponedException>(
                () => rAction.ControlPanel(functionId.Instance.Value).Result!.Restart()
            );
            
            var (status, postponedUntil) = await store.GetFunction(rAction.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FlowId(flowType, "schedule_re-invoke");
            await store.CreateFunction(
                rAction.MapToStoredId(functionId), 
                "humanInstanceId",
                param: "hello".ToJson().ToUtf8Bytes(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null
            ).ShouldBeTrueAsync();

            await rAction.ControlPanel(functionId.Instance).Result!.ScheduleRestart();
            await BusyWait.Until(() => store.GetFunction(rAction.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(rAction.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
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
            flowType, Task (string _, Workflow _) =>
            {
                Postpone.Throw(postponeFor: TimeSpan.FromSeconds(10));
                return Task.CompletedTask;
            });

        //invoke
        {
            var functionId = new FlowId(flowType, "invoke");
            Should.Throw<InvocationPostponedException>(
                () => rAction.Invoke(functionId.Instance.Value, "hello")
            );
            var (status, postponedUntil) = await store.GetFunction(rAction.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);            
        }
        //schedule
        {
            var functionId = new FlowId(flowType, "schedule");
            await rAction.Schedule("schedule", "hello");
            await BusyWait.Until(() => store.GetFunction(rAction.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store
                .GetFunction(rAction.MapToStoredId(functionId))
                .Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //re-invoke
        {
            var functionId = new FlowId(flowType, "re-invoke");
            await store.CreateFunction(
                rAction.MapToStoredId(functionId), 
                "humanInstanceId",
                param: "hello".ToJson().ToUtf8Bytes(), 
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null
            ).ShouldBeTrueAsync();
            
            Should.Throw<InvocationPostponedException>(
                () => rAction.ControlPanel(functionId.Instance).Result!.Restart()
            );
            
            var (status, postponedUntil) = await store.GetFunction(rAction.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FlowId(flowType, "schedule_re-invoke");
            await store.CreateFunction(
                rAction.MapToStoredId(functionId), 
                "humanInstanceId",
                param: "hello".ToJson().ToUtf8Bytes(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null
            ).ShouldBeTrueAsync();

            await rAction.ControlPanel(functionId.Instance).Result!.ScheduleRestart();
            await BusyWait.Until(() => store.GetFunction(rAction.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(rAction.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
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
            Should.Throw<InvocationPostponedException>(
                () => rFunc.Invoke("invoke", "hello")
            );
            var (status, postponedUntil) = await store
                .GetFunction(rFunc.MapToStoredId(new FlowId(flowType, "invoke")))
                .Map(sf => Tuple.Create(sf?.Status, sf?.Expires));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule
        {
            var functionId = new FlowId(flowType, "schedule");

            await rFunc.Schedule("schedule", "hello");
            await BusyWait.Until(() => store.GetFunction(rFunc.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Postponed));
            var (status, postponedUntil) = await store
                .GetFunction(rFunc.MapToStoredId(functionId))
                .Map(sf => Tuple.Create(sf?.Status, sf?.Expires));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //re-invoke
        {
            var functionId = new FlowId(flowType, "re-invoke");
            await store.CreateFunction(
                rFunc.MapToStoredId(functionId), 
                "humanInstanceId",
                param: "hello".ToJson().ToUtf8Bytes(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null
            ).ShouldBeTrueAsync();
            var controlPanel = await rFunc.ControlPanel(functionId.Instance).ShouldNotBeNullAsync();
            Should.Throw<InvocationPostponedException>(() => controlPanel.Restart());
            
            var (status, postponedUntil) = await store.GetFunction(rFunc.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FlowId(flowType, "schedule_re-invoke");
            await store.CreateFunction(
                rFunc.MapToStoredId(functionId), 
                "humanInstanceId",
                param: "hello".ToJson().ToUtf8Bytes(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanel(functionId.Instance).ShouldNotBeNullAsync();
            await controlPanel.ScheduleRestart();

            await BusyWait.Until(() => store.GetFunction(rFunc.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(rFunc.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
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
            Should.Throw<InvocationPostponedException>(
                () => rFunc.Invoke(functionId.Instance.Value, "hello")
            );
            var (status, postponedUntil) = await store.GetFunction(rFunc.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);            
        }
        //schedule
        {
            var functionId = new FlowId(flowType, "schedule");

            await rFunc.Schedule("schedule", "hello");
            await BusyWait.Until(() => store.GetFunction(rFunc.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Postponed));
            var (status, postponedUntil) = await store
                .GetFunction(rFunc.MapToStoredId(functionId))
                .Map(sf => Tuple.Create(sf?.Status, sf?.Expires));

            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //re-invoke
        {
            var functionId = new FlowId(flowType, "re-invoke");
            await store.CreateFunction(
                rFunc.MapToStoredId(functionId), 
                "humanInstanceId",
                "hello".ToJson().ToUtf8Bytes(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanel(functionId.Instance).ShouldNotBeNullAsync();
            Should.Throw<InvocationPostponedException>(() => controlPanel.Restart());
            
            var (status, postponedUntil) = await store.GetFunction(rFunc.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        //schedule re-invoke
        {
            var functionId = new FlowId(flowType, "schedule_re-invoke");
            await store.CreateFunction(
                rFunc.MapToStoredId(functionId), 
                "humanInstanceId",
                param: "hello".ToJson().ToUtf8Bytes(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanel(functionId.Instance).ShouldNotBeNullAsync();
            await controlPanel.ScheduleRestart();

            await BusyWait.Until(() => store.GetFunction(rFunc.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(rFunc.MapToStoredId(functionId)).Map(sf => Tuple.Create(sf?.Status, sf?.Expires));
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
        var storedType = await store.TypeStore.InsertOrGetStoredType(functionId.Type);
        var storedId = new StoredId(storedType, functionId.Instance.Value.ToStoredInstance());
        await store.CreateFunction(
            storedId, 
            "humanInstanceId",
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            storedId,
            postponeUntil: DateTime.UtcNow.AddDays(-1).Ticks,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToUtf8Bytes().ToFunc(), LeaseLength: 0)
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
        var registration = functionsRegistry.RegisterAction(
            flowType,
            Task (string param) =>
            {
                syncedParam.Value = param; 
                invokedFlag.Raise();
                return Task.CompletedTask;
            });
        
        await BusyWait.Until(() => invokedFlag.IsRaised, maxWait: TimeSpan.FromSeconds(10));
        syncedParam.Value.ShouldBe("hello");
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    private class State : FlowState
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
                    return Task.CompletedTask;
                });

        await rAction.ScheduleAt(
            functionId.Instance.ToString(),
            "param",
            delayUntil: DateTime.UtcNow.AddSeconds(1)
        );

        var sf = await store.GetFunction(rAction.MapToStoredId(functionId)).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Postponed);

        var controlPanel = await rAction.ControlPanel(functionId.Instance);
        controlPanel.ShouldNotBeNull();
        await BusyWait.Until(async () =>
            {
                await controlPanel.Refresh();
                return controlPanel.Status == Status.Succeeded;
            });

        flag.IsRaised.ShouldBeTrue();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
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

        var sf = await store.GetFunction(rFunc.MapToStoredId(functionId)).ShouldNotBeNullAsync();
        sf.Status.ShouldBe(Status.Postponed);

        var controlPanel = await rFunc.ControlPanel(functionId.Instance);
        controlPanel.ShouldNotBeNull();
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });
        flag.IsRaised.ShouldBeTrue();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
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
                (string _, Workflow workflow) => workflow.Delay(TimeSpan.FromDays(1))
            );

        await Should.ThrowAsync<InvocationPostponedException>(
            () => rFunc.Invoke(functionId.Instance.Value, "test")
        );
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
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
                (string _, Workflow workflow) => workflow.Delay(tomorrow)
            );

        await Should.ThrowAsync<InvocationPostponedException>(
            () => registration.Invoke(functionId.Instance.Value, "test")
        );

        var controlPanel = await registration.ControlPanel(functionId.Instance);
        controlPanel.ShouldNotBeNull();
        
        unhandledExceptionHandler.ShouldNotHaveExceptions();
    }
}