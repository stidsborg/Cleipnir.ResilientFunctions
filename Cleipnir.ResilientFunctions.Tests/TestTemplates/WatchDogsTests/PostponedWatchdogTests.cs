using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Watchdogs;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests
{
    public abstract class PostponedWatchdogTests
    {
        private readonly FunctionTypeId _functionTypeId = "functionId".ToFunctionTypeId();
        private readonly FunctionInstanceId _instanceId = "instanceId".ToFunctionInstanceId();
        private FunctionId FunctionId => new FunctionId(_functionTypeId, _instanceId);

        public abstract Task PostponedFunctionInvocationIsCompletedByWatchDog();
        protected async Task PostponedFunctionInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
            var syncedScrapbook = new Synced<RScrapbook>();
            
            using var watchDog = new PostponedWatchdog<string>(
                _functionTypeId,
                (param, scrapbook) =>
                {
                    syncedScrapbook.Value = scrapbook;
                    return Funcs.ToUpper(param.ToString()!);
                },
                store,
                new RFuncInvoker(
                    store, 
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    unhandledExceptionHandler
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                unhandledExceptionHandler
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store.SetFunctionState(
                FunctionId,
                Status.Postponed,
                scrapbookJson: null,
                result: null,
                failed: null,
                postponedUntil: DateTime.UtcNow.AddMinutes(-1).Ticks,
                expectedEpoch: 0
            ).ShouldBeTrueAsync();
            
            _ = watchDog.Start();
            
            await BusyWait.Until(
                async () => await store.GetFunctionsWithStatus(_functionTypeId, Status.Succeeded).Any()
            );

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction!.Result!.Deserialize().ShouldBe("HELLO");
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
            syncedScrapbook.Value.ShouldBeNull();
        }
        
        public abstract Task PostponedFunctionWithScrapbookInvocationIsCompletedByWatchDog();
        protected async Task PostponedFunctionWithScrapbookInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
            
            using var watchDog = new PostponedWatchdog<string>(
                _functionTypeId,
                async (param, scrapbook) =>
                {
                    ((Scrapbook) scrapbook!).Value = 1;
                    await scrapbook.Save();
                    return ((string) param).ToUpper().ToSucceededRResult();
                },
                store,
                new RFuncInvoker(
                    store, 
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    unhandledExceptionHandler
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                unhandledExceptionHandler
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: typeof(Scrapbook).SimpleQualifiedName(),
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store.SetFunctionState(
                FunctionId,
                Status.Postponed,
                new Scrapbook().ToJson(),
                result: null,
                failed: null,
                postponedUntil: DateTime.UtcNow.AddMinutes(-1).Ticks,
                expectedEpoch: 0
            ).ShouldBeTrueAsync();
            
            _ = watchDog.Start();

            await BusyWait.Until(
                async () => await store.GetFunction(FunctionId).Map(sf => sf!.Status) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction!.Result!.Deserialize().ShouldBe("HELLO");

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
        }
        
        public abstract Task PostponedActionInvocationIsCompletedByWatchDog();
        protected async Task PostponedActionInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
            var syncedParam = new Synced<string>();
            var syncedScrapbook = new Synced<RScrapbook>();
            
            using var watchDog = new PostponedWatchdog(
                _functionTypeId,
                (param, scrapbook) =>
                {
                    syncedScrapbook.Value = scrapbook;
                    syncedParam.Value = (string) param;
                    return RResult.Success.ToTask();
                },
                store,
                new RFuncInvoker(
                    store, 
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    unhandledExceptionHandler
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                unhandledExceptionHandler
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store.SetFunctionState(
                FunctionId,
                Status.Postponed,
                scrapbookJson: null,
                result: null,
                failed: null,
                postponedUntil: DateTime.UtcNow.AddMinutes(-1).Ticks,
                expectedEpoch: 0
            ).ShouldBeTrueAsync();
            
            _ = watchDog.Start();
            
            await BusyWait.Until(
                async () => await store.GetFunctionsWithStatus(_functionTypeId, Status.Succeeded).Any()
            );

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Result.ShouldBeNull();
            storedFunction.Scrapbook.ShouldBeNull();
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
            syncedParam.Value.ShouldBe("hello");
            syncedScrapbook.Value.ShouldBeNull();
        }
        
        public abstract Task PostponedActionWithScrapbookInvocationIsCompletedByWatchDog();
        protected async Task PostponedActionWithScrapbookInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
            var syncedParam = new Synced<object>();
            
            using var watchDog = new PostponedWatchdog(
                _functionTypeId,
                async (param, scrapbook) =>
                {
                    syncedParam.Value = param;
                    ((Scrapbook) scrapbook!).Value = 1;
                    await scrapbook.Save();
                    return RResult.Success;
                },
                store,
                new RFuncInvoker(
                    store, 
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    unhandledExceptionHandler
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                unhandledExceptionHandler
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: typeof(Scrapbook).SimpleQualifiedName(),
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store.SetFunctionState(
                FunctionId,
                Status.Postponed,
                new Scrapbook().ToJson(),
                result: null,
                failed: null,
                postponedUntil: DateTime.UtcNow.AddMinutes(-1).Ticks,
                expectedEpoch: 0
            ).ShouldBeTrueAsync();
            
            _ = watchDog.Start();

            await BusyWait.Until(
                async () => await store.GetFunction(FunctionId).Map(sf => sf!.Status) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction!.Result.ShouldBeNull();

            storedFunction.Scrapbook.ShouldNotBeNull();
            storedFunction.Scrapbook.Deserialize().CastTo<Scrapbook>().Value.ShouldBe(1);
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
            (syncedParam.Value is string).ShouldBeTrue();
        }

        private class Scrapbook : RScrapbook
        {
            public int Value { get; set; }
        }
    }
}