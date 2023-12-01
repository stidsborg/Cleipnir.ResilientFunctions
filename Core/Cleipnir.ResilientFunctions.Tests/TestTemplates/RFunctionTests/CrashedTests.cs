using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive;
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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        signOfLifeFrequency: TimeSpan.Zero, 
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterFunc(
                    functionTypeId,
                    (string _) => NeverCompletingTask.OfType<string>()
                ).Invoke;

            _ = nonCompletingRFunctions(functionInstanceId.Value, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.FromMilliseconds(250)
                )
            );

            var rFunc = rFunctions
                .RegisterFunc(
                    functionTypeId,
                    (string s) => s.ToUpper().ToTask()
                ).Invoke;
            
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rFunc(functionInstanceId.Value, param).ShouldBeAsync("TEST");
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
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
            var nonCompletingRFunctions = rFunctions    
                .RegisterFunc(
                    functionTypeId,
                    (string _, Scrapbook _) => NeverCompletingTask.OfType<Result<string>>()
                ).Invoke;

            _ = nonCompletingRFunctions(functionInstanceId.Value, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.FromMilliseconds(250)
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
            
            await BusyWait.Until(async () => 
                await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status == Status.Succeeded),
                maxWait: TimeSpan.FromSeconds(5)
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            await rFunc(functionInstanceId.Value, param).ShouldBeAsync("TEST");
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    public abstract Task NonCompletedActionIsCompletedByWatchDog();
    protected async Task NonCompletedActionIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        signOfLifeFrequency: TimeSpan.Zero, 
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterAction(
                    functionTypeId,
                    (string _) => NeverCompletingTask.OfVoidType
                )
                .Invoke;

            _ = nonCompletingRFunctions(functionInstanceId.Value, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.FromMilliseconds(250)
                )
            );

            var rAction = rFunctions
                .RegisterAction(
                    functionTypeId,
                    (string _) => Task.CompletedTask
                )
                .Invoke;
            
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) == Status.Succeeded
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rAction(functionInstanceId.Value, param);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }

    public abstract Task NonCompletedActionWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedActionWithScrapbookIsCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var nonCompletingRFunctions = new RFunctions
                (
                    store, 
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        signOfLifeFrequency: TimeSpan.Zero, 
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                )
                .RegisterAction(
                    functionTypeId,
                    (string _, Scrapbook _) => NeverCompletingTask.OfVoidType
                ).Invoke;

            _ = nonCompletingRFunctions(functionInstanceId.Value, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    signOfLifeFrequency: TimeSpan.FromMilliseconds(250)
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

            await BusyWait.Until(async () =>
                await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status == Status.Succeeded),
                maxWait: TimeSpan.FromSeconds(5)
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            await rAction(functionInstanceId.Value, param);
        }

        if (unhandledExceptionHandler.ThrownExceptions.Any())
            throw new Exception("Unhandled exception occurred", unhandledExceptionHandler.ThrownExceptions[0]);
    }
    
    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}