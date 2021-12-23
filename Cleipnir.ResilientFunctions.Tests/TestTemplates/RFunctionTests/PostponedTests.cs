using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
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
            var function = new PostponedFunctionWrapper(shouldPostpone: true);
            var rFunc = RFunctions
                .Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string s) => function.Func(s),
                    _ => _
                );

            var result = await rFunc(param);
            result.Postponed.ShouldBeTrue();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            var function = new PostponedFunctionWrapper(shouldPostpone: false);
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => function.Func(s),
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
            var function = new PostponedFunctionWrapper(shouldPostpone: true);
            var rFunc = RFunctions
                .Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => function.Func(s, scrapbook),
                    _ => _
                );

            var result = await rFunc(param);
            result.Postponed.ShouldBeTrue();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            var function = new PostponedFunctionWrapper(shouldPostpone: false);
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => function.Func(s, scrapbook),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(2);
            
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
            var function = new PostponedFunctionWrapper(shouldPostpone: true);
            var rFunc = RFunctions
                .Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string s) => function.Action(s),
                    _ => _
                );

            var result = await rFunc(param);
            result.Postponed.ShouldBeTrue();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            var function = new PostponedFunctionWrapper(shouldPostpone: false);
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => function.Action(s),
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
            var function = new PostponedFunctionWrapper(shouldPostpone: true);
            var rFunc = RFunctions
                .Create(
                    store,
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => function.Action(s, scrapbook),
                    _ => _
                );

            var result = await rFunc(param);
            result.Postponed.ShouldBeTrue();
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
        {
            var function = new PostponedFunctionWrapper(shouldPostpone: false);
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => function.Action(s, scrapbook),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(2);

            var result = await rFunc(param);
            result.Succeeded.ShouldBe(true);
            unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        }
    }

    private class PostponedFunctionWrapper
    {
        private readonly bool _shouldPostpone;

        public PostponedFunctionWrapper(bool shouldPostpone) => _shouldPostpone = shouldPostpone;

        public async Task<RResult<string>> Func(string s)
        {
            await Task.Delay(0);

            if (_shouldPostpone)
                return Postpone.For(TimeSpan.FromMilliseconds(100));

            return s.ToUpper();
        }

        public async Task<RResult<string>> Func(string s, Scrapbook scrapbook)
        {
            scrapbook.Value = 1;
            await scrapbook.Save();

            if (_shouldPostpone)
                return Postpone.For(TimeSpan.FromMilliseconds(100));

            scrapbook.Value = 2;
            await scrapbook.Save();
            return s.ToUpper();
        }

        public async Task<RResult> Action(string s)
        {
            await Task.Delay(0);

            if (_shouldPostpone)
                return Postpone.For(TimeSpan.FromMilliseconds(100));

            return RResult.Success;
        }

        public async Task<RResult> Action(string s, Scrapbook scrapbook)
        {
            scrapbook.Value = 1;
            await scrapbook.Save();

            if (_shouldPostpone)
                return Postpone.For(TimeSpan.FromMilliseconds(100));
            
            scrapbook.Value = 2;
            await scrapbook.Save();
            
            return RResult.Success;
        }
    }

    private class Scrapbook : RScrapbook
    {
        public int Value { get; set; }
    }
}