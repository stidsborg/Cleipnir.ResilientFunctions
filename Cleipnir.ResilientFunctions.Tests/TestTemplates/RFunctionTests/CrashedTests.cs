using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class CrashedTests
{
    public abstract Task NonCompletedFuncIsCompletedByWatchDog();
    protected async Task NonCompletedFuncIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(NonCompletedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var wrapper = new BlockingFunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, unhandledExceptionHandler.Catch, crashedCheckFrequency: TimeSpan.Zero)
                .Register(
                    functionTypeId,
                    (string s) => wrapper.Func(s),
                    _ => _
                );

            _ = nonCompletingRFunctions(param);
        }
        {
            var wrapper = new BlockingFunctionWrapper(false);
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => wrapper.Func(s),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) != Status.Executing
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            await rFunc(param).EnsureSuccess().ShouldBeAsync("TEST");
        }

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }

    public abstract Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedFuncWithScrapbookIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(NonCompletedFuncWithScrapbookIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var wrapper = new BlockingFunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, unhandledExceptionHandler.Catch, crashedCheckFrequency: TimeSpan.Zero)
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => wrapper.Func(s, scrapbook),
                    _ => _
                );

            _ = nonCompletingRFunctions(param);
        }
        {
            var wrapper = new BlockingFunctionWrapper(false);
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => wrapper.Func(s, scrapbook),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) != Status.Executing
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(2);
            await rFunc(param).EnsureSuccess().ShouldBeAsync("TEST");
        }

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task NonCompletedActionIsCompletedByWatchDog();
    protected async Task NonCompletedActionIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(NonCompletedActionIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var wrapper = new BlockingFunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, unhandledExceptionHandler.Catch, crashedCheckFrequency: TimeSpan.Zero)
                .Register(
                    functionTypeId,
                    (string s) => wrapper.Action(s),
                    _ => _
                );

            _ = nonCompletingRFunctions(param);
        }
        {
            var wrapper = new BlockingFunctionWrapper(false);
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
            );

            var rAction = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => wrapper.Action(s),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) != Status.Executing
            );

            var status = await store.GetFunction(functionId).Map(f => f?.Status);
            status.ShouldBe(Status.Succeeded);
            var rResult = await rAction(param);
            rResult.EnsureSuccess();
        }

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }

    public abstract Task NonCompletedActionWithScrapbookIsCompletedByWatchDog();
    protected async Task NonCompletedActionWithScrapbookIsCompletedByWatchDog(IFunctionStore store)
    {
        var functionTypeId = nameof(NonCompletedFuncIsCompletedByWatchDog).ToFunctionTypeId();
        const string param = "test";
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            var wrapper = new BlockingFunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, unhandledExceptionHandler.Catch, crashedCheckFrequency: TimeSpan.Zero)
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => wrapper.Action(s, scrapbook),
                    _ => _
                );

            _ = nonCompletingRFunctions(param);
        }
        {
            var wrapper = new BlockingFunctionWrapper(false);
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionHandler.Catch,
                TimeSpan.FromMilliseconds(2)
            );

            var rAction = rFunctions
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => wrapper.Action(s, scrapbook),
                    _ => _
                );

            var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
            await BusyWait.Until(
                async () => await store
                    .GetFunction(functionId)
                    .Map(f => f?.Status ?? Status.Failed) != Status.Executing
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(2);
            var rResult = await rAction(param);
            rResult.EnsureSuccess();
        }

        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    private class BlockingFunctionWrapper
    {
        private readonly bool _shouldBlock;

        public BlockingFunctionWrapper(bool shouldBlock) => _shouldBlock = shouldBlock;

        public async Task<RResult<string>> Func(string s)
        {
            if (_shouldBlock)
                await TaskUtils.NeverCompleting;

            return s.ToUpper();
        }

        public async Task<RResult<string>> Func(string s, Scrapbook scrapbook)
        {
            scrapbook.Value = 1;
            await scrapbook.Save();

            if (_shouldBlock)
                await TaskUtils.NeverCompleting;

            scrapbook.Value = 2;
            await scrapbook.Save();
            
            return s.ToUpper();
        }

        public async Task<RResult> Action(string s)
        {
            if (_shouldBlock)
                await TaskUtils.NeverCompleting;

            return RResult.Success;
        }

        public async Task<RResult> Action(string s, Scrapbook scrapbook)
        {
            scrapbook.Value = 1;
            await scrapbook.Save();

            if (_shouldBlock)
                await TaskUtils.NeverCompleting;

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