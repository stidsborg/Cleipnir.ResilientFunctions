using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ExceptionHandling;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.ShutdownCoordination;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Watchdogs;
using Cleipnir.ResilientFunctions.Watchdogs.Invocation;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests
{
    public abstract class CrashedWatchdogTests
    {
        private readonly FunctionTypeId _functionTypeId = "functionId".ToFunctionTypeId();
        private readonly FunctionInstanceId _instanceId = "instanceId".ToFunctionInstanceId();
        private FunctionId FunctionId => new FunctionId(_functionTypeId, _instanceId);

        public abstract Task CrashedFunctionInvocationIsCompletedByWatchDog();
        protected async Task CrashedFunctionInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
            var syncedScrapbook = new Synced<RScrapbook>();
            
            using var watchDog = new CrashedWatchdog<string>(
                _functionTypeId,
                (param, scrapbook) =>
                {
                    syncedScrapbook.Value = scrapbook;
                    return Funcs.ToUpper(param.ToString()!);
                },
                store,
                new RFuncInvoker(
                    store, 
                    new DefaultSerializer(),
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    unhandledExceptionHandler,
                    new ShutdownCoordinator()
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                unhandledExceptionHandler,
                new ShutdownCoordinator()
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            _ = watchDog.Start();

            await BusyWait.Until(
                async () => await store.GetFunction(FunctionId).Map(sf => sf!.Status) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction!.Result!.DefaultDeserialize().ShouldBe("HELLO");
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
            syncedScrapbook.Value.ShouldBeNull();
        }
        
        public abstract Task CrashedFunctionWithScrapbookInvocationIsCompletedByWatchDog();
        protected async Task CrashedFunctionWithScrapbookInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);

            using var watchDog = new CrashedWatchdog<string>(
                _functionTypeId,
                async (param, scrapbook) =>
                {
                    ((Scrapbook) scrapbook!).Value = 1;
                    await scrapbook.Save();
                    return ((string) param).ToUpper();
                },
                store,
                new RFuncInvoker(
                    store, 
                    new DefaultSerializer(),
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    unhandledExceptionHandler,
                    new ShutdownCoordinator()
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                unhandledExceptionHandler,
                new ShutdownCoordinator()
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: typeof(Scrapbook).SimpleQualifiedName(),
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            _ = watchDog.Start();

            await BusyWait.Until(
                async () => await store.GetFunction(FunctionId).Map(sf => sf!.Status) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction!.Result!.DefaultDeserialize().ShouldBe("HELLO");
            
            storedFunction.Scrapbook.ShouldNotBeNull();
            var scrapbook = (Scrapbook) storedFunction.Scrapbook.DefaultDeserialize();
            scrapbook.Value.ShouldBe(1);
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
            scrapbook.ShouldNotBeNull();
        }
        
        public abstract Task CrashedActionInvocationIsCompletedByWatchDog();
        protected async Task CrashedActionInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
            var syncedParam = new Synced<string>();
            var syncedScrapbook = new Synced<RScrapbook>();
            
            using var watchDog = new CrashedWatchdog(
                _functionTypeId,
                (param, scrapbook) =>
                {
                    syncedScrapbook.Value = scrapbook;
                    syncedParam.Value = (string) param;
                    return RResult.Success.ToTask();
                },
                store,
                new RActionInvoker(
                    store, 
                    new DefaultSerializer(),
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    unhandledExceptionHandler,
                    new ShutdownCoordinator()
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                unhandledExceptionHandler,
                new ShutdownCoordinator()
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            _ = watchDog.Start();
            
            await BusyWait.Until(
                async () => await store.GetFunction(FunctionId).Map(sf => sf!.Status) == Status.Succeeded
            );
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
            syncedParam.Value.ShouldBe("hello");
            syncedScrapbook.Value.ShouldBeNull();
        }
        
        public abstract Task CrashedActionWithScrapbookInvocationIsCompletedByWatchDog();
        protected async Task CrashedActionWithScrapbookInvocationIsCompletedByWatchDog(IFunctionStore store)
        {
            var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
            var unhandledExceptionHandler = new UnhandledExceptionHandler(unhandledExceptionCatcher.Catch);
            var syncedParam = new Synced<string>();

            using var watchDog = new CrashedWatchdog(
                _functionTypeId,
                async (param, scrapbook) =>
                {
                    syncedParam.Value = (string) param;
                    ((Scrapbook) scrapbook!).Value = 1;
                    await scrapbook.Save();
                    return RResult.Success;
                },
                store,
                new RActionInvoker(
                    store, 
                    new DefaultSerializer(),
                    new NeverExecutingSignOfLifeUpdaterFactory(),
                    unhandledExceptionHandler,
                    new ShutdownCoordinator()
                ),
                checkFrequency: TimeSpan.FromMilliseconds(1),
                unhandledExceptionHandler,
                new ShutdownCoordinator()
            );

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter("hello".ToJson(), typeof(string).SimpleQualifiedName()),
                scrapbookType: typeof(Scrapbook).SimpleQualifiedName(),
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            _ = watchDog.Start();

            await BusyWait.Until(
                async () => await store.GetFunction(FunctionId).Map(sf => sf!.Status) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Scrapbook.ShouldNotBeNull();
            var scrapbook = (Scrapbook) storedFunction.Scrapbook.DefaultDeserialize();
            scrapbook.Value.ShouldBe(1);
            
            unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
            syncedParam.Value.ShouldBe("hello");
        }

        private class Scrapbook : RScrapbook
        {
            public int Value { get; set; }
        }
    }
}