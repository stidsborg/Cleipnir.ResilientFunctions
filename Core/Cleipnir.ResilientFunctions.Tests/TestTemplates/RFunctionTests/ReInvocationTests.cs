using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils.Scrapbooks;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class ReInvocationTests
{
    public abstract Task ActionReInvocationSunshineScenario();
    protected async Task ActionReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                CrashedCheckFrequency: TimeSpan.Zero,
                PostponedCheckFrequency: TimeSpan.Zero
            )
        );
        var syncedParameter = new Synced<string>();

        var rFunc = rFunctions
            .RegisterAction(
                functionType, (string s) =>
                {
                    if (flag.Position == FlagPosition.Lowered)
                    {
                        flag.Raise();
                        throw new Exception("oh no");
                    }

                    syncedParameter.Value = s;
                }
            );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        await rFunc.ReInvoke(
            "something",
            new[] {Status.Failed}
        );
        
        syncedParameter.Value.ShouldBe("something");

        var function = await store.GetFunction(new FunctionId(functionType, "something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ActionWithScrapbookReInvocationSunshineScenario();
    protected async Task ActionWithScrapbookReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                CrashedCheckFrequency: TimeSpan.Zero,
                PostponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rAction = rFunctions.RegisterAction<string, ListScrapbook<string>>(
            functionType,
            async (param, scrapbook) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    scrapbook.List.Add("hello");
                    await scrapbook.Save();
                    flag.Raise();
                    throw new Exception("oh no");
                }
                scrapbook.List.Add("world");
            }
        );

        await Should.ThrowAsync<Exception>(() => rAction.Invoke("something", "something"));

        var syncedListFromScrapbook = new Synced<List<string>>();
        await rAction.ReInvoke(
            "something",
            new[] {Status.Failed},
            scrapbookUpdater: scrapbook =>
            {
                syncedListFromScrapbook.Value = new List<string>(scrapbook.List);
                scrapbook.List.Clear();
            }
        );

        var function = await store.GetFunction(new FunctionId(functionType, "something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var scrapbook = function.Scrapbook!.ScrapbookJson!.DeserializeFromJsonTo<ListScrapbook<string>>();
        scrapbook.List.Single().ShouldBe("world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScrapbookUpdaterIsCalledBeforeReInvokeOnAction();
    protected async Task ScrapbookUpdaterIsCalledBeforeReInvokeOnAction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var functionId = new FunctionId(functionType, "something");
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rAction = rFunctions.RegisterAction<string, Scrapbook>(
            functionType,
            inner: (_, scrapbook) =>
            {
                scrapbook.Value++;
                if (flag.Position == FlagPosition.Lowered)
                    throw new Exception("oh no");
            }
        );

        await Should.ThrowAsync<Exception>(() => rAction.Invoke("something", "something"));
        var sfScrapbook = await store
            .GetFunction(functionId)
            .Map(sf => sf?.Scrapbook?.ScrapbookJson?.DeserializeFromJsonTo<Scrapbook>());

        sfScrapbook.ShouldNotBeNull();
        sfScrapbook.Value.ShouldBe(1);
        
        flag.Raise();
        await rAction.ReInvoke(
            functionInstanceId: "something",
            expectedStatuses: new[] {Status.Failed},
            expectedEpoch: 0,
            scrapbookUpdater: s => s.Value = -1 
        );

        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var scrapbook = function.Scrapbook!.ScrapbookJson!.DeserializeFromJsonTo<Scrapbook>();
        scrapbook.Value.ShouldBe(0);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc();
    protected async Task ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var functionId = new FunctionId(functionType, "something");
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(store, new Settings(unhandledExceptionCatcher.Catch));
        
        var rFunc = rFunctions.RegisterFunc<string, Scrapbook, string>(
            functionType,
            inner: (param, scrapbook) =>
            {
                scrapbook.Value++;
                if (flag.Position == FlagPosition.Lowered)
                    throw new Exception("oh no");

                return param;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));
        var sfScrapbook = await store
            .GetFunction(functionId)
            .Map(sf => sf?.Scrapbook?.ScrapbookJson?.DeserializeFromJsonTo<Scrapbook>());

        sfScrapbook.ShouldNotBeNull();
        sfScrapbook.Value.ShouldBe(1);
        
        flag.Raise();
        var returned = await rFunc.ReInvoke(
            functionInstanceId: "something",
            expectedStatuses: new[] {Status.Failed},
            expectedEpoch: 0,
            scrapbookUpdater: s => s.Value = -1 
        );
        returned.ShouldBe("something");
        
        var function = await store.GetFunction(functionId);
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        var scrapbook = function.Scrapbook!.ScrapbookJson!.DeserializeFromJsonTo<Scrapbook>();
        scrapbook.Value.ShouldBe(0);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }

    public abstract Task FuncReInvocationSunshineScenario();
    protected async Task FuncReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                CrashedCheckFrequency: TimeSpan.Zero,
                PostponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionType,
            async s =>
            {
                await Task.CompletedTask;
                if (flag.Position == FlagPosition.Lowered)
                {
                    flag.Raise();
                    throw new Exception("oh no");
                }
                return s;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));
        
        await rFunc.ReInvoke(
            "something",
            new[] {Status.Failed}
        );

        var function = await store.GetFunction(new FunctionId(functionType, "something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result!.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task FuncWithScrapbookReInvocationSunshineScenario();
    protected async Task FuncWithScrapbookReInvocationSunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var flag = new SyncedFlag();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                CrashedCheckFrequency: TimeSpan.Zero,
                PostponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = rFunctions.RegisterFunc<string, ListScrapbook<string>, string>(
            functionType,
            async (param, scrapbook) =>
            {
                if (flag.Position == FlagPosition.Lowered)
                {
                    scrapbook.List.Add("hello");
                    await scrapbook.Save();
                    flag.Raise();
                    throw new Exception("oh no");
                }
                scrapbook.List.Add("world");
                return param;
            }
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        var scrapbookList = new Synced<List<string>>();

        var result = await rFunc.ReInvoke(
            "something",
            new[] {Status.Failed},
            scrapbookUpdater: scrapbook =>
            {
                scrapbookList.Value = new List<string>(scrapbook.List);
                scrapbook.List.Clear();
            }
        );
        
        result.ShouldBe("something");

        var function = await store.GetFunction(new FunctionId(functionType, "something"));
        function.ShouldNotBeNull();
        function.Status.ShouldBe(Status.Succeeded);
        function.Result!.ResultJson!.DeserializeFromJsonTo<string>().ShouldBe("something");
        var scrapbook = function.Scrapbook!.ScrapbookJson!.DeserializeFromJsonTo<ListScrapbook<string>>();
        scrapbook.List.Single().ShouldBe("world");
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ReInvocationFailsWhenItHasUnexpectedStatus();
    protected async Task ReInvocationFailsWhenItHasUnexpectedStatus(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                CrashedCheckFrequency: TimeSpan.Zero,
                PostponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = rFunctions.RegisterAction(
            functionType,
            (string _) => Task.FromException(new Exception("oh no"))
        );

        await Should.ThrowAsync<Exception>(() => rFunc.Invoke("something", "something"));

        await Should.ThrowAsync<Exception>(() =>
            rFunc.ReInvoke("something", new[] {Status.Executing})
        );

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task ReInvocationFailsWhenTheFunctionDoesNotExist();
    protected async Task ReInvocationFailsWhenTheFunctionDoesNotExist(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                CrashedCheckFrequency: TimeSpan.Zero,
                PostponedCheckFrequency: TimeSpan.Zero
            )
        );

        var rFunc = rFunctions.RegisterAction(
            functionType,
            (string _) => Task.FromException(new Exception("oh no"))
        );

        await Should.ThrowAsync<Exception>(() =>
            rFunc.ReInvoke("something", new[] {Status.Executing})
        );

        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task ReInvocationFailsWhenTheFunctionIsAtUnsupportedVersion();
    protected async Task ReInvocationFailsWhenTheFunctionIsAtUnsupportedVersion(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        const string functionType = "someFunctionType";
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var rFunctions = new RFunctions(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.Zero
                )
            );

            var rFunc = rFunctions.RegisterFunc(
                functionType,
                (string _) => NeverCompletingTask.OfType<string>(),
                version: 2
            ).Schedule;
            await rFunc("instance", "hello world");
        }

        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.Zero
                )
            );

            var registration = rFunctions.RegisterFunc(
                functionType,
                (string _) => NeverCompletingTask.OfType<string>(),
                version: 1
            );
            await Should.ThrowAsync<UnexpectedFunctionState>(
                () => registration.Invoke("instance", "hello world")
            );
            await Should.ThrowAsync<UnexpectedFunctionState>(
                () => registration.ReInvoke("instance", new[] {Status.Executing})
            );
            await Should.ThrowAsync<UnexpectedFunctionState>(
                () => registration.ScheduleReInvocation("instance", new[] {Status.Executing})
            );
        }
    }
}