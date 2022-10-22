using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class CrashedTests
{
    public abstract Task NonCompletedFuncIsCompletedByWatchDog();
    protected async Task NonCompletedFuncIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(NonCompletedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        crashedCheckFrequency: TimeSpan.Zero, 
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterFunc(
                    functionTypeId,
                    (string _) => NeverCompletingTask.OfType<string>()
                ).Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.FromMilliseconds(2)
                )
            );

            var rFunc = rFunctions
                .RegisterFunc(
                    functionTypeId,
                    (string s) => s.ToUpper().ToTask()
                ).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rFunc(param, param).ShouldBeAsync("TEST");
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(NonCompletedFuncWithScrapbookIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        crashedCheckFrequency: TimeSpan.Zero, 
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterFunc(
                    functionTypeId,
                    (string _, Scrapbook _) => NeverCompletingTask.OfType<Result<string>>()
                ).Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.FromMilliseconds(2)
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
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            await rFunc(param, param).ShouldBeAsync("TEST");
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    public abstract Task NonCompletedActionIsCompletedByWatchDog();
    protected async Task NonCompletedActionIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(NonCompletedActionIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        crashedCheckFrequency: TimeSpan.Zero, 
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterAction(
                    functionTypeId,
                    (string _) => NeverCompletingTask.OfVoidType
                )
                .Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.FromMilliseconds(2)
                )
            );

            var rAction = rFunctions
                .RegisterAction(
                    functionTypeId,
                    (string _) => Task.CompletedTask
                )
                .Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rAction(param, param);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedActionWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedActionWithScrapbookIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(NonCompletedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        crashedCheckFrequency: TimeSpan.Zero, 
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterAction(
                    functionTypeId,
                    (string _, Scrapbook _) => NeverCompletingTask.OfVoidType
                ).Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.FromMilliseconds(2)
                )
            );

            var rAction = rFunctions
                .RegisterAction(
                    functionTypeId,
                    async (string _, Scrapbook scrapbook) =>
                    {
                        scrapbook.Value = 1;
                        await scrapbook.Save();
                    }
                ).Invoke;

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            await rAction(param, param);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    public abstract Task CrashedActionIsNotInvokedOnHigherVersion();
    protected async Task CrashedActionIsNotInvokedOnHigherVersion(Task<IFunctionStore> storeTask)
    {
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var functionId = new FunctionId(
            functionTypeId: nameof(CrashedActionIsNotInvokedOnHigherVersion),
            functionInstanceId: "test"
        );
        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 10,
            version: 2
        ).ShouldBeTrueAsync();

        using var rFunctions = new RFunctions
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(10),
                postponedCheckFrequency: TimeSpan.Zero
            )
        );
        rFunctions.RegisterAction(functionId.TypeId, (string _) => { });

        await Task.Delay(500);
        var sf = await store.GetFunction(functionId);
        sf.ShouldNotBeNull();
        sf.Status.ShouldBe(Status.Executing);
        sf.Version.ShouldBe(2);
    }
    
    public abstract Task CrashedActionReInvocationModeShouldBeRetry();
    protected Task CrashedActionReInvocationModeShouldBeRetry(Task<IFunctionStore> storeTask)
    {
        //todo refactor when invocation mode is supported again
        return Task.CompletedTask;
        /*var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var store = await storeTask;
        var functionId = new FunctionId(
            functionTypeId: nameof(CrashedActionReInvocationModeShouldBeRetry),
            functionInstanceId: "test"
        );
        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            crashedCheckFrequency: 10,
            version: 0
        ).ShouldBeTrueAsync();

        using var rFunctions = new RFunctions
        (
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(10),
                PostponedCheckFrequency: TimeSpan.Zero
            )
        );

        var syncedInvocationMode = new Synced<InvocationMode>();
        rFunctions.RegisterAction(
            functionId.TypeId,
            (string _) => syncedInvocationMode.Value = ResilientInvocation.Mode
        );

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf!.Status == Status.Succeeded)
        );
        syncedInvocationMode.Value.ShouldBe(InvocationMode.Retry);*/
    }

    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}