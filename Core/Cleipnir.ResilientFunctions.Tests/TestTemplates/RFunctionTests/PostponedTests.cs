﻿using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using UnitScrapbook = Cleipnir.ResilientFunctions.Helpers.UnitScrapbook;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class PostponedTests
{
    public abstract Task PostponedFuncIsCompletedByWatchDog();
    protected async Task PostponedFuncIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(PostponedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var rFunctions = new RFunctions
                (
                    crashableStore,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        signOfLifeFrequency: TimeSpan.Zero,
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                );
            var rFunc = rFunctions.RegisterFunc<string, string>(
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
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
                )
            );

            var rFunc = rFunctions
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
    
    public abstract Task PostponedFuncWithScrapbookIsCompletedByWatchDog();
    protected async Task PostponedFuncWithScrapbookIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(PostponedFuncWithScrapbookIsCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var rFunctions = new RFunctions
                (
                    store,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        signOfLifeFrequency: TimeSpan.Zero,
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                );
            var rFunc = rFunctions.RegisterFunc<string, Scrapbook, string>(
                    functionTypeId,
                    (_, _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1_000))
                )
                .Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rFunc(param, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
                )
            );

            var rFunc = rFunctions
                .RegisterFunc(
                    functionTypeId,
                    async (string s, Scrapbook scrapbook) =>
                    {
                        scrapbook.Value = 1;
                        await scrapbook.Save();
                        return s.ToUpper();
                    }
                ).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            
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
            using var rFunctions = new RFunctions
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
            );
            var rAction = rFunctions.RegisterAction(
                functionTypeId,
                (string _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1_000))
            ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rAction(functionInstanceId.Value, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );

            var rFunc = rFunctions
                .RegisterFunc(
                    functionTypeId,
                    (string s) => s.ToUpper().ToTask()
                ).Invoke;
            
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            await rFunc(functionInstanceId.Value, param);
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionWithScrapbookIsCompletedByWatchDog();
    protected async Task PostponedActionWithScrapbookIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var rFunctions = new RFunctions
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
            );
            var rAction = rFunctions.RegisterAction(
                functionTypeId,
                (string _, Scrapbook _) => Postpone.For(1_000)
            ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => 
                rAction(functionInstanceId.Value, param)
            );
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(10)
                )
            );
            
            var rFunc = rFunctions
                .RegisterAction(
                    functionTypeId,
                    async (string _, Scrapbook scrapbook) =>
                    {
                        scrapbook.Value = 1;
                        await scrapbook.Save();
                    }
                ).Invoke;
            
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);

            await rFunc(functionInstanceId.Value, param);
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionIsCompletedAfterInMemoryTimeout();
    protected async Task PostponedActionIsCompletedAfterInMemoryTimeout(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = new FunctionId(
            functionTypeId: nameof(PostponedFuncWithScrapbookIsCompletedByWatchDog),
            functionInstanceId: "test"
        );
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var flag = new SyncedFlag();

        using var rFunctions = new RFunctions
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                signOfLifeFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.FromSeconds(10)
            )
        );
        var rFunc = rFunctions
            .RegisterAction(
                functionId.TypeId,
                (string _) =>
                {
                    if (flag.Position == FlagPosition.Raised) return Result.Succeed;

                    flag.Raise();
                    return Postpone.For(1_000);
                }).Invoke;
        
        _ = rFunc(functionId.InstanceId.ToString(), "param");

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);

        await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
    }
    
    public abstract Task PostponedActionIsCompletedByWatchDogAfterCrash();
    protected async Task PostponedActionIsCompletedByWatchDogAfterCrash(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var rFunctions = new RFunctions
            (
                crashableStore,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.FromSeconds(10)
                )
            );
            var rFunc = rFunctions
                .RegisterAction(functionId.TypeId, (string _) => Postpone.For(1_000))
                .Invoke;

            var instanceId = functionId.InstanceId.ToString();
            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => _ = rFunc(instanceId, "param"));
            crashableStore.Crash();
        }
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var rFunctions = new RFunctions
            (
                crashableStore,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            rFunctions.RegisterAction(functionId.TypeId, (string _) => { });
            
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
        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rAction = rFunctions.RegisterAction(
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
                new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
                storedEvents: null,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null
            ).ShouldBeTrueAsync();
            
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.ControlPanels.For(functionId.InstanceId.Value).Result!.ReInvoke()
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
                new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
                storedEvents: null,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null
            ).ShouldBeTrueAsync();

            await rAction.ControlPanels.For(functionId.InstanceId).Result!.ScheduleReInvoke();
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Postponed));
            
            var (status, postponedUntil) = await store.GetFunction(functionId).Map(sf => Tuple.Create(sf?.Status, sf?.PostponedUntil));
            status.ShouldBe(Status.Postponed);
            postponedUntil.HasValue.ShouldBeTrue();
            postponedUntil!.Value.ShouldBeGreaterThan(DateTime.UtcNow.Add(TimeSpan.FromSeconds(5)).Ticks);
            postponedUntil.Value.ShouldBeLessThanOrEqualTo(DateTime.UtcNow.Add(TimeSpan.FromSeconds(10)).Ticks);
        }
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ThrownPostponeExceptionResultsInPostponedActionWithScrapbook();
    protected async Task ThrownPostponeExceptionResultsInPostponedActionWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var functionTypeId = nameof(ThrownPostponeExceptionResultsInPostponedActionWithScrapbook);
        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            (string _, UnitScrapbook _) => Postpone.Throw(postponeFor: TimeSpan.FromSeconds(10))
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
                new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                new StoredScrapbook(new UnitScrapbook().ToJson(), typeof(UnitScrapbook).SimpleQualifiedName()),
                storedEvents: null,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null
            ).ShouldBeTrueAsync();
            
            Should.Throw<FunctionInvocationPostponedException>(
                () => rAction.ControlPanels.For(functionId.InstanceId).Result!.ReInvoke()
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
                new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                new StoredScrapbook(new UnitScrapbook().ToJson(), typeof(UnitScrapbook).SimpleQualifiedName()),
                storedEvents: null,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null
            ).ShouldBeTrueAsync();

            await rAction.ControlPanels.For(functionId.InstanceId).Result!.ScheduleReInvoke();
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
        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rFunc = rFunctions.RegisterFunc<string, string>(
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
                new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
                storedEvents: null,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null
            ).ShouldBeTrueAsync();
            var controlPanel = await rFunc.ControlPanels.For(functionId.InstanceId).ShouldNotBeNullAsync();
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
                new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
                storedEvents: null,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanels.For(functionId.InstanceId).ShouldNotBeNullAsync();
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
    
    public abstract Task ThrownPostponeExceptionResultsInPostponedFuncWithScrapbook();
    protected async Task ThrownPostponeExceptionResultsInPostponedFuncWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var functionTypeId = TestFunctionId.Create().TypeId;
        using var rFunctions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: unhandledExceptionCatcher.Catch)
        );
        var rFunc = rFunctions.RegisterFunc<string, string>(
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
                new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
                storedEvents: null,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanels.For(functionId.InstanceId).ShouldNotBeNullAsync();
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
                new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
                storedEvents: null,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null
            ).ShouldBeTrueAsync();

            var controlPanel = await rFunc.ControlPanels.For(functionId.InstanceId).ShouldNotBeNullAsync();
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

        var storedParameter = new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName());
        var storedScrapbook = new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName());
        
        await store.CreateFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            storedEvents: null,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null
        ).ShouldBeTrueAsync();

        await store.PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.AddDays(-1).Ticks,
            scrapbookJson: new RScrapbook().ToJson(),
            expectedEpoch: 0,
            complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
        ).ShouldBeTrueAsync();

        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler: unhandledExceptionCatcher.Catch,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(10)
            )
        );

        var syncedParam = new Synced<string>();
        var invokedFlag = new SyncedFlag();
        rFunctions.RegisterAction(
            functionTypeId,
            void (string param) =>
            {
                syncedParam.Value = param; 
                invokedFlag.Raise();
            });

        await BusyWait.UntilAsync(() => invokedFlag.IsRaised, maxWait: TimeSpan.FromSeconds(10));
        //await Task.Delay(5_000);

        //var pfs = await store.GetPostponedFunctions(functionTypeId.Value, expiresBefore: DateTime.Now.Add(TimeSpan.FromHours(1)).Ticks).ToListAsync();
        //await invokedFlag.WaitForRaised(maxWaitMs: 1_000_000);
        syncedParam.Value.ShouldBe("hello");
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}