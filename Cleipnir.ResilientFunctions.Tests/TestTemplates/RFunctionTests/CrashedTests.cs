using System;
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
                    unhandledExceptionHandler.Catch, 
                    crashedCheckFrequency: TimeSpan.Zero, 
                    postponedCheckFrequency: TimeSpan.Zero
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
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(2)
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

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
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
                    unhandledExceptionHandler.Catch, 
                    crashedCheckFrequency: TimeSpan.Zero, 
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .RegisterFuncWithScrapbook(
                    functionTypeId,
                    (string _, Scrapbook _) => NeverCompletingTask.OfType<Result<string>>()
                ).Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .RegisterFuncWithScrapbook(
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

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
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
                    unhandledExceptionHandler.Catch, 
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
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
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
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

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
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
                    unhandledExceptionHandler.Catch, 
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .RegisterActionWithScrapbook(
                    functionTypeId,
                    (string _, Scrapbook _) => NeverCompletingTask.OfVoidType
                ).Invoke;

            _ = nonCompletingRFunctions(param, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
            );

            var rAction = rFunctions
                .RegisterActionWithScrapbook(
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

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }

    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}