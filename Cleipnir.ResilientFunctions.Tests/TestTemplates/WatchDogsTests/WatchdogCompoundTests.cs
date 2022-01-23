using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public abstract class WatchdogCompoundTests 
{
    public abstract Task FunctionCompoundTest();
    public async Task FunctionCompoundTest(IFunctionStore store)
    {
        var functionTypeId = nameof(FunctionCompoundTest).ToFunctionTypeId();
        const string functionInstanceId = "SomeId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            //first invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();

            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    paramTcs.TrySetResult(p);
                    return NeverCompletingTask.OfType<RResult<string>>();
                },
                p => p.Id
            );

            _ = rFunc(param);

            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        {
            //second invocation is delayed
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var afterNextSetFunctionState = crashableStore
                .AfterSetFunctionStateStream
                .Take(1)
                .ObserveOnThreadPool();
            
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    paramTcs.TrySetResult(p);
                    return TimeSpan.FromMilliseconds(10).ToPostponedRResult<string>().ToTask();
                },
                p => p.Id
            );

            await afterNextSetFunctionState;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        { 
            //third invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    return NeverCompletingTask.OfType<RResult<string>>();
                },
                p => p.Id
            );

            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);
            crashableStore.Crash();
        }
        {
            //fourth invocation succeeds
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p) => $"{p.Id}-{p.Value}".ToSucceededRResult().ToTask(),
                p => p.Id
            );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result!.DefaultDeserialize().CastTo<string>().ShouldBe($"{param.Id}-{param.Value}");
        }
    }

    public abstract Task FunctionWithScrapbookCompoundTest();
    public async Task FunctionWithScrapbookCompoundTest(IFunctionStore store)
    {
        var functionTypeId = nameof(FunctionWithScrapbookCompoundTest).ToFunctionTypeId();
        const string functionInstanceId = "SomeId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            //first invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (Param p, Scrapbook scrapbook) =>
                {
                    scrapbook.Scraps.Add(1);
                    return scrapbook
                        .Save()
                        .ContinueWith(_ => paramTcs.TrySetResult(p))
                        .ContinueWith(_ => NeverCompletingTask.OfType<RResult<string>>())
                        .Unwrap();
                },
                p => p.Id
            );

            _ = rFunc(param);
            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        {
            //second invocation is delayed
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();

            var afterNextPostponedSetFunctionState = crashableStore
                .AfterSetFunctionStateStream
                .Where(p => p.PostponedUntil != null)
                .Take(1)
                .ObserveOnThreadPool();
            
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                async (Param p, Scrapbook scrapbook) =>
                {
                    paramTcs.TrySetResult(p);
                    scrapbook.Scraps.Add(2);
                    await scrapbook.Save();
                    return Postpone.For(10);
                },
                p => p.Id
            );
            
            await afterNextPostponedSetFunctionState;
            crashableStore.Crash();
            
            paramTcs.Task.Result.ShouldBe(param);
        }
        {
            //third invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p, Scrapbook scrapbook) =>
                {
                    scrapbook.Scraps.Add(3);
                    var savedTask = scrapbook.Save();
                    return savedTask.ContinueWith(_ =>
                    {
                        Task.Run(() => paramTcs.TrySetResult(p));
                        return NeverCompletingTask.OfType<RResult<string>>();
                    }).Unwrap();
                },
                p => p.Id
            );

            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);
            crashableStore.Crash();
        }
        {
            //fourth invocation succeeds
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                async (Param p, Scrapbook scrapbook) =>
                {
                    scrapbook.Scraps.Add(4);
                    await scrapbook.Save();
                    return $"{p.Id}-{p.Value}".ToSucceededRResult();
                },
                p => p.Id
            );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result!.DefaultDeserialize()
                .CastTo<string>()
                .ShouldBe($"{param.Id}-{param.Value}");
            storedFunction.Scrapbook!.DefaultDeserialize()
                .CastTo<Scrapbook>()
                .Scraps
                .SequenceEqual(new [] {1,2,3,4})
                .ShouldBeTrue();
        }
    }

    public abstract Task ActionCompoundTest();
    public async Task ActionCompoundTest(IFunctionStore store)
    {
        var functionTypeId = nameof(ActionCompoundTest).ToFunctionTypeId();
        const string functionInstanceId = "SomeId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        //first invocation crashes
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var tcs = new TaskCompletionSource<Param>();
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    tcs.TrySetResult(p);
                    return NeverCompletingTask.OfType<RResult>();
                },
                p => p.Id
            );
            
            _ = rFunc(param);
            var actualParam = await tcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        //second invocation is delayed
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var afterSetFunctionState = crashableStore
                .AfterSetFunctionStateStream
                .Take(1)
                .ObserveOnThreadPool();
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    paramTcs.TrySetResult(p);
                    return TimeSpan.FromMilliseconds(10).ToPostponedRResult().ToTask();
                },
                p => p.Id
            );

            await afterSetFunctionState;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        //third invocation crashes
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var invocationStarted = new TaskCompletionSource();
            var paramTcs = new TaskCompletionSource<Param>();
            var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    paramTcs.TrySetResult(p);
                    Task.Run(invocationStarted.SetResult);
                    return NeverCompletingTask.OfType<RResult>();
                },
                p => p.Id
            );

            await invocationStarted.Task;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        //fourth invocation succeeds
        {
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    paramTcs.TrySetResult(p);
                    return RResult.Success.ToTask();
                },
                p => p.Id
            );
            
            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );
            
            paramTcs.Task.Result.ShouldBe(param);
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result.ShouldBeNull();
        }
    }

    public abstract Task ActionWithScrapbookCompoundTest();
    public async Task ActionWithScrapbookCompoundTest(IFunctionStore store)
    {
        var functionTypeId = nameof(ActionWithScrapbookCompoundTest).ToFunctionTypeId();
        const string functionInstanceId = "SomeId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            //first invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (Param p, Scrapbook scrapbook) =>
                {
                    scrapbook.Scraps.Add(1);
                    return scrapbook
                        .Save()
                        .ContinueWith(_ => Task.Run(() => paramTcs.TrySetResult(p)))
                        .ContinueWith(_ => NeverCompletingTask.OfType<RResult>())
                        .Unwrap();
                },
                p => p.Id
            );

            _ = rFunc(param);
            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        {
            //second invocation is delayed
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var afterNextPostponed = crashableStore
                .AfterSetFunctionStateStream
                .Where(p => p.PostponedUntil != null)
                .Take(1)
                .ObserveOnThreadPool();
            
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                async (Param p, Scrapbook scrapbook) =>
                {
                    paramTcs.TrySetResult(p);
                    scrapbook.Scraps.Add(2);
                    await scrapbook.Save();
                    return Postpone.For(10);
                },
                p => p.Id
            );
            
            await afterNextPostponed;
            crashableStore.Crash();
            
            paramTcs.Task.Result.ShouldBe(param);
        }
        {
            //third invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var invocationStarted = new TaskCompletionSource();
            var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p, Scrapbook scrapbook) =>
                {
                    scrapbook.Scraps.Add(3);
                    var savedTask = scrapbook.Save();
                    Task.Run(() => paramTcs.TrySetResult(p));
                    return savedTask.ContinueWith(_ =>
                    {
                        Task.Run(invocationStarted.SetResult);
                        return NeverCompletingTask.OfType<RResult>();
                    }).Unwrap();
                },
                p => p.Id
            );
            
            await invocationStarted.Task;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        {
            //fourth invocation succeeds
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                async (Param p, Scrapbook scrapbook) =>
                {
                    paramTcs.TrySetResult(p);
                    scrapbook.Scraps.Add(4);
                    await scrapbook.Save();
                    return RResult.Success;
                },
                p => p.Id
            );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );

            paramTcs.Task.Result.ShouldBe(param);
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result.ShouldBeNull();
            storedFunction.Scrapbook!.DefaultDeserialize()
                .CastTo<Scrapbook>()
                .Scraps
                .SequenceEqual(new [] {1,2,3,4})
                .ShouldBeTrue();
        }
    }

    private class Scrapbook : RScrapbook
    {
        public List<int> Scraps { get; }

        public Scrapbook() => Scraps = new List<int>();
        public Scrapbook(List<int> scraps) => Scraps = scraps;
    }
    
    private record Param(string Id, int Value);
}