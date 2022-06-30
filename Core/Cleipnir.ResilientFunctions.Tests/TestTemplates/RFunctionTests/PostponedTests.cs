using System;
using System.Diagnostics;
using System.Threading.Tasks;
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
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var rFunctions = new FunctionContainer
                (
                    crashableStore,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.Zero,
                        PostponedCheckFrequency: TimeSpan.Zero
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
            using var rFunctions = new FunctionContainer(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.FromMilliseconds(2)
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
            using var rFunctions = new FunctionContainer
                (
                    store,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        CrashedCheckFrequency: TimeSpan.Zero,
                        PostponedCheckFrequency: TimeSpan.Zero
                    )
                );
            var rFunc = rFunctions.RegisterFunc(
                    functionTypeId,
                    Result<string> (string _, Scrapbook _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1_000))
                )
                .Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rFunc(param, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new FunctionContainer(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.FromMilliseconds(2)
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
        var functionTypeId = nameof(PostponedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var rFunctions = new FunctionContainer
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.Zero
                )
            );
            var rAction = rFunctions.RegisterAction(
                functionTypeId,
                (string _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1_000))
            ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rAction(param, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new FunctionContainer(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.FromMilliseconds(2)
                )
            );

            var rFunc = rFunctions
                .RegisterFunc(
                    functionTypeId,
                    (string s) => s.ToUpper().ToTask()
                ).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            await rFunc(param, param);
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionWithScrapbookIsCompletedByWatchDog();
    protected async Task PostponedActionWithScrapbookIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(PostponedFuncWithScrapbookIsCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var rFunctions = new FunctionContainer
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.Zero
                )
            );
            var rAction = rFunctions.RegisterAction(
                functionTypeId,
                (string _, Scrapbook _) => Postpone.For(1_000)
            ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => 
                rAction(param, param)
            );
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new FunctionContainer(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.FromMilliseconds(10)
                )
            );

            var counter = 0;
            var rFunc = rFunctions
                .RegisterAction(
                    functionTypeId,
                    async (string _, Scrapbook scrapbook) =>
                    {
                        counter++;
                        if (counter == 2)
                            Console.WriteLine("OH NO");
                        Console.WriteLine("OK");
                        Console.WriteLine(new StackTrace());
                        scrapbook.Value = 1;
                        await scrapbook.Save();
                    }
                ).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);

            await rFunc(param, param);
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

        using var rFunctions = new FunctionContainer
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                CrashedCheckFrequency: TimeSpan.Zero,
                PostponedCheckFrequency: TimeSpan.FromSeconds(10)
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
        var functionId = new FunctionId(
            functionTypeId: nameof(PostponedFuncWithScrapbookIsCompletedByWatchDog),
            functionInstanceId: "test"
        );
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var rFunctions = new FunctionContainer
            (
                crashableStore,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.FromSeconds(10)
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
            using var rFunctions = new FunctionContainer
            (
                crashableStore,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    CrashedCheckFrequency: TimeSpan.Zero,
                    PostponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            rFunctions.RegisterAction(functionId.TypeId, (string _) => { });
            
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        
            await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
        }
    }

    private class Scrapbook : Domain.Scrapbook
    {
        public int Value { get; set; }
    }
}