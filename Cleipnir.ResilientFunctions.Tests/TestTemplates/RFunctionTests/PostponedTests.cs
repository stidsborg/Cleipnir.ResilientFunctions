using System;
using System.Runtime.InteropServices.ComTypes;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
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
            var rFunc = new RFunctions
                (
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _) => _.ToTask(),
                    preInvoke: null,
                    postInvoke: (_, _) => Postpone.For(1, inProcessWait: false)
                ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rFunc(param, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
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
            var rFunc = new RFunctions
                (
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string p, Scrapbook _) => p.ToTask(),
                    preInvoke: null,
                    postInvoke: (_, _, _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1), false)
                ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rFunc(param, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
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
            var rAction = new RFunctions
                (
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _) => Task.CompletedTask,
                    preInvoke: null,
                    postInvoke: (_, _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1), inProcessWait: false)
                ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rAction(param, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
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
            var rFunc = new RFunctions
                (
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _, Scrapbook _) => Task.CompletedTask, 
                    preInvoke: null,
                    postInvoke: (_, _, _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1), inProcessWait: false)
                ).Invoke;

            await Should.ThrowAsync<FunctionInvocationPostponedException>(() => rFunc(param, param));
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    async (string _, Scrapbook scrapbook) =>
                    {
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
    
    public abstract Task InMemoryPostponedActionIsNotCompletedByWatchDog();
    protected async Task InMemoryPostponedActionIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(InMemoryPostponedActionIsNotCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var rAction = new RFunctions
                (
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _) => Task.CompletedTask,
                    preInvoke: null,
                    postInvoke: (_, _) => Postpone.Until(DateTime.UtcNow.AddMilliseconds(1000), inProcessWait: true)
                ).Invoke;

            _ = rAction(param, param);
        }
        {
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var _ = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => s.ToUpper().ToTask()
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await Task.Delay(500);
            await store
                .GetFunction(functionId)
                .Map(s => s?.Status)
                .ShouldBeAsync(Status.Executing);
            
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }

    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}