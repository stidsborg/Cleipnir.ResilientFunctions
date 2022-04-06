using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public abstract class WatchdogCompoundTests 
{
    public abstract Task FunctionCompoundTest();
    public async Task FunctionCompoundTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(FunctionCompoundTest).ToFunctionTypeId();
        const string functionInstanceId = "SomeId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            //first invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();

            using var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    return NeverCompletingTask.OfType<Return<string>>();
                }
            ).Invoke;

            _ = rFunc(param.Id, param);

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
            
            using var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    return "".ToTask();
                }, 
                preInvoke: null,
                postInvoke: async (returned, metadata) =>
                {
                    await Task.CompletedTask;
                    return Postpone.For(10, inProcessWait: false);
                });

            await afterNextSetFunctionState;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        { 
            //third invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    return NeverCompletingTask.OfType<string>();
                }
            );

            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);
            crashableStore.Crash();
        }
        {
            //fourth invocation succeeds
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param p) => $"{p.Id}-{p.Value}".ToTask()
            );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result!.DefaultDeserialize()!.CastTo<string>().ShouldBe($"{param.Id}-{param.Value}");
        }
    }

    public abstract Task FunctionWithScrapbookCompoundTest();
    public async Task FunctionWithScrapbookCompoundTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(FunctionWithScrapbookCompoundTest).ToFunctionTypeId();
        const string functionInstanceId = "SomeId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            //first invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch
            );
            var rFunc = rFunctions.ActionWithScrapbook(
                functionTypeId,
                async (Param p, Scrapbook scrapbook) =>
                {
                    scrapbook.Scraps.Add(1);
                    await scrapbook.Save();
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    await NeverCompletingTask.OfType<string>();
                }
            ).Register().Invoke;

            _ = rFunc(param.Id, param);
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
            
            using var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.ActionWithScrapbook(
                    functionTypeId,
                    async (Param p, Scrapbook scrapbook) =>
                    {
                        _ = Task.Run(() => paramTcs.TrySetResult(p));
                        scrapbook.Scraps.Add(2);
                        await scrapbook.Save();
                    })
                .WithPostInvoke(async (_, _, _) =>
                {
                    await Task.CompletedTask;
                    return Postpone.For(10, inProcessWait: false);
                })
                .Register();
            
            await afterNextPostponedSetFunctionState;
            crashableStore.Crash();
            
            paramTcs.Task.Result.ShouldBe(param);
        }
        {
            //third invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.ActionWithScrapbook(
                functionTypeId,
                async (Param p, Scrapbook scrapbook) =>
                {
                    scrapbook.Scraps.Add(3);
                    await scrapbook.Save();
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    await NeverCompletingTask.OfType<string>();
                }
            ).Register();

            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);
            crashableStore.Crash();
        }
        {
            //fourth invocation succeeds
            using var rFunctions = new RFunctions(
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
                    return $"{p.Id}-{p.Value}";
                }
            );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result!.DefaultDeserialize()!
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
    public async Task ActionCompoundTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ActionCompoundTest).ToFunctionTypeId();
        const string functionInstanceId = "SomeId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        //first invocation crashes
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var tcs = new TaskCompletionSource<Param>();
            using var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch
            );
            var rAction = rFunctions
                .Action(
                    functionTypeId,
                    inner: (Param p) =>
                    {
                        tcs.TrySetResult(p);
                        return NeverCompletingTask.OfVoidType();
                    })
                .Register()
                .Invoke;
            
            _ = rAction(param.Id, param);
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
            using var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions
                .Action(
                    functionTypeId,
                    inner: (Param p) => Task.Run(() => paramTcs.TrySetResult(p))
                )
                .WithPostInvoke((_, _) => Postpone.For(10, inProcessWait: false))
                .Register();

            await afterSetFunctionState;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        //third invocation crashes
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var invocationStarted = new TaskCompletionSource();
            var paramTcs = new TaskCompletionSource<Param>();
            var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Action(
                functionTypeId,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    Task.Run(invocationStarted.SetResult);
                    return NeverCompletingTask.OfVoidType();
                }
            ).Register();

            await invocationStarted.Task;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        //fourth invocation succeeds
        {
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions
                .Action(
                    functionTypeId,
                (Param p) => Task.Run(() => paramTcs.TrySetResult(p))
                )
                .Register();
            
            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );
            
            paramTcs.Task.Result.ShouldBe(param);
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result.ShouldBeNull();
        }
    }

    public abstract Task ActionWithScrapbookCompoundTest();
    public async Task ActionWithScrapbookCompoundTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ActionWithScrapbookCompoundTest).ToFunctionTypeId();
        const string functionInstanceId = "SomeId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            //first invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch
            );
            var rFunc = rFunctions.ActionWithScrapbook(
                functionTypeId,
                async (Param p, Scrapbook scrapbook) =>
                {
                    scrapbook.Scraps.Add(1);
                    await scrapbook.Save();
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    await NeverCompletingTask.OfVoidType();
                }
            ).Register().Invoke;

            _ = rFunc(param.Id, param);
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
            
            using var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions
                .ActionWithScrapbook(
                    functionTypeId,
                    async (Param p, Scrapbook scrapbook) =>
                    {
                        _ = Task.Run(() => paramTcs.TrySetResult(p));
                        scrapbook.Scraps.Add(2);
                        await scrapbook.Save();
                    })
                .WithPostInvoke(async (_, _, _) =>
                {
                    await Task.CompletedTask;
                    return Postpone.For(10, inProcessWait: false);
                })
                .Register();
            
            await afterNextPostponed;
            crashableStore.Crash();
            
            paramTcs.Task.Result.ShouldBe(param);
        }
        {
            //third invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var invocationStarted = new TaskCompletionSource();
            var rFunctions = new RFunctions(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.ActionWithScrapbook(
                functionTypeId,
                async (Param p, Scrapbook scrapbook) =>
                {
                    scrapbook.Scraps.Add(3);
                    var savedTask = scrapbook.Save();
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    _ = Task.Run(() => invocationStarted.SetResult());
                    await savedTask;
                    await NeverCompletingTask.OfVoidType();
                }
            ).Register();
            
            await invocationStarted.Task;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        {
            //fourth invocation succeeds
            var paramTcs = new TaskCompletionSource<Param>();
            using var rFunctions = new RFunctions(
                store,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.ActionWithScrapbook(
                functionTypeId,
                async (Param p, Scrapbook scrapbook) =>
                {
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    scrapbook.Scraps.Add(4);
                    await scrapbook.Save();
                }
            ).Register();

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