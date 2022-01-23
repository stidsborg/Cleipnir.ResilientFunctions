using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class PostponedTests
{
    public abstract Task PostponedFuncIsCompletedByWatchDog();
    protected async Task PostponedFuncIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(PostponedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var rFunc = RFunctions
                .Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _) => TimeSpan.FromMilliseconds(1).ToPostponedRResult<string>().ToTask(),
                    _ => _
                );

            var result = await rFunc(param);
            result.Postponed.ShouldBeTrue();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => s.ToUpper().ToSucceededRResult().ToTask(),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            await rFunc(param).EnsureSuccess().ShouldBeAsync("TEST");
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedFuncWithScrapbookIsCompletedByWatchDog();
    protected async Task PostponedFuncWithScrapbookIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(PostponedFuncWithScrapbookIsCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var rFunc = RFunctions
                .Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _, Scrapbook _) => TimeSpan.FromMilliseconds(1).ToPostponedRResult<string>().ToTask(),
                    _ => _
                );

            var result = await rFunc(param);
            result.Postponed.ShouldBeTrue();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = RFunctions.Create(
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
                        return s.ToUpper().ToSucceededRResult();
                    },
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            
            await rFunc(param).EnsureSuccess().ShouldBeAsync("TEST");
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionIsCompletedByWatchDog();
    protected async Task PostponedActionIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(PostponedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var rFunc = RFunctions
                .Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _) => TimeSpan.FromMilliseconds(1).ToPostponedRResult().ToTask(),
                    _ => _
                );

            var result = await rFunc(param);
            result.Postponed.ShouldBeTrue();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => s.ToUpper().ToSucceededRResult().ToTask(),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            var result = await rFunc(param);
            result.Succeeded.ShouldBeTrue();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }
    
    public abstract Task PostponedActionWithScrapbookIsCompletedByWatchDog();
    protected async Task PostponedActionWithScrapbookIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(PostponedFuncWithScrapbookIsCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            var rFunc = RFunctions
                .Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string _, Scrapbook _) => TimeSpan.FromMilliseconds(1).ToPostponedRResult().ToTask(),
                    _ => _
                );

            var result = await rFunc(param);
            result.Postponed.ShouldBeTrue();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            using var rFunctions = RFunctions.Create(
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
                        return RResult.Success;
                    },
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.DefaultDeserialize().CastTo<Scrapbook>().Value.ShouldBe(1);

            var result = await rFunc(param);
            result.Succeeded.ShouldBe(true);
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }

    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}