using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, _) = functionId;
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            //first invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();

            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rFunc = functionsRegistry.RegisterFunc(
                functionTypeId,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    return NeverCompletingTask.OfType<Result<string>>();
                }
            ).Invoke;

            _ = rFunc(functionId.InstanceId.Value, param);

            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        {
            //second invocation is delayed
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var afterNextSetFunctionState = crashableStore.AfterPostponeFunctionStream.Take(1);

            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterFunc<Param, string>(
                functionTypeId,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    return Postpone.For(100);
                });
            
            await afterNextSetFunctionState;
            crashableStore.Crash();
            await Task.Yield();
            paramTcs.Task.Result.ShouldBe(param);
        }
        { 
            //third invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterFunc(
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
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterFunc(
                functionTypeId,
                (Param p) => $"{p.Id}-{p.Value}".ToTask()
            );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result.DefaultDeserialize()!.CastTo<string>().ShouldBe($"{param.Id}-{param.Value}");
        }
    }

    public abstract Task FunctionWithStateCompoundTest();
    public async Task FunctionWithStateCompoundTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, _) = functionId;
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            //first invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rFunc = functionsRegistry.RegisterFunc(
                functionTypeId,
                async (Param p, WorkflowState state) =>
                {
                    state.Scraps.Add(1);
                    await state.Save();
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    return await NeverCompletingTask.OfType<string>();
                }
            ).Invoke;

            _ = rFunc(functionId.InstanceId.Value, param);
            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        {
            //second invocation is delayed
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();

            var afterNextPostponedSetFunctionState = crashableStore.AfterPostponeFunctionStream.Take(1);

            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterFunc<Param, WorkflowState, string>(  //explicit generic parameters to satisfy Rider-ide
                functionTypeId,
                async (p, state) =>
                {
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    state.Scraps.Add(2);
                    await state.Save();
                    return Postpone.For(100);
                });
            
            await afterNextPostponedSetFunctionState;
            crashableStore.Crash();
            await Task.Yield();
            paramTcs.Task.Result.ShouldBe(param);
        }
        {
            //third invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterFunc(
                functionTypeId,
                async (Param p, WorkflowState state) =>
                {
                    state.Scraps.Add(3);
                    await state.Save();
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    return await NeverCompletingTask.OfType<string>();
                }
            );

            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);
            crashableStore.Crash();
        }
        {
            //fourth invocation succeeds
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterFunc(
                functionTypeId,
                async (Param p, WorkflowState state) =>
                {
                    state.Scraps.Add(4);
                    await state.Save();
                    return $"{p.Id}-{p.Value}";
                }
            );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded,
                maxWait: TimeSpan.FromSeconds(5)
            );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result.DefaultDeserialize()!
                .CastTo<string>()
                .ShouldBe($"{param.Id}-{param.Value}");
            storedFunction.State.DefaultDeserialize()
                .CastTo<WorkflowState>()
                .Scraps
                .ShouldBe(new [] {1,2,3,4});
        }
    }

    public abstract Task ActionCompoundTest();
    public async Task ActionCompoundTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, _) = functionId;
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        //first invocation crashes
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var tcs = new TaskCompletionSource<Param>();
            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rAction = functionsRegistry
                .RegisterAction(
                    functionTypeId,
                    inner: (Param p) =>
                    {
                        tcs.TrySetResult(p);
                        return NeverCompletingTask.OfVoidType;
                    })
                .Invoke;
            
            _ = rAction(functionId.InstanceId.Value, param);
            var actualParam = await tcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        //second invocation is delayed
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var afterSetFunctionState = crashableStore.AfterPostponeFunctionStream.Take(1);
            
            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry
                .RegisterAction(
                    functionTypeId,
                    inner: (Param p) =>
                    {
                        Task.Run(() => paramTcs.TrySetResult(p));
                        return Postpone.For(100);
                    }
                );

            await afterSetFunctionState;
            crashableStore.Crash();
            await Task.Yield();
            paramTcs.Task.Result.ShouldBe(param);
        }
        //third invocation crashes
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var invocationStarted = new TaskCompletionSource();
            var paramTcs = new TaskCompletionSource<Param>();
            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterAction(
                functionTypeId,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    Task.Run(invocationStarted.SetResult);
                    return NeverCompletingTask.OfVoidType;
                }
            );

            await invocationStarted.Task;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        //fourth invocation succeeds
        {
            var paramTcs = new TaskCompletionSource<Param>();
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry
                .RegisterAction(
                    functionTypeId,
                (Param p) => Task.Run(() => paramTcs.TrySetResult(p))
                );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );
            
            paramTcs.Task.Result.ShouldBe(param);
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result.ResultType.ShouldBe(typeof(Unit).SimpleQualifiedName());
        }
    }

    public abstract Task ActionWithStateCompoundTest();
    public async Task ActionWithStateCompoundTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, _) = functionId;
        var param = new Param("SomeId", 25);
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            //first invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rFunc = functionsRegistry.RegisterAction(
                functionTypeId,
                async (Param p, WorkflowState state) =>
                {
                    state.Scraps.Add(1);
                    await state.Save();
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    await NeverCompletingTask.OfVoidType;
                }
            ).Invoke;

            _ = rFunc(functionId.InstanceId.Value, param);
            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        {
            //second invocation is delayed
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var afterNextPostponed = crashableStore.AfterPostponeFunctionStream.Take(1);

            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry
                .RegisterAction(
                    functionTypeId,
                    async (Param p, WorkflowState state, Workflow __) =>
                    {
                        _ = Task.Run(() => paramTcs.TrySetResult(p));
                        state.Scraps.Add(2);
                        await state.Save();
                        return Postpone.For(100);
                    });
            
            await afterNextPostponed;
            crashableStore.Crash();
            await Task.Yield();
            paramTcs.Task.Result.ShouldBe(param);
        }
        {
            //third invocation crashes
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            var invocationStarted = new TaskCompletionSource();
            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterAction(
                functionTypeId,
                async (Param p, WorkflowState state) =>
                {
                    state.Scraps.Add(3);
                    var savedTask = state.Save();
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    _ = Task.Run(() => invocationStarted.SetResult());
                    await savedTask;
                    await NeverCompletingTask.OfVoidType;
                }
            );
            
            await invocationStarted.Task;
            crashableStore.Crash();
            paramTcs.Task.Result.ShouldBe(param);
        }
        {
            //fourth invocation succeeds
            var paramTcs = new TaskCompletionSource<Param>();
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterAction(
                functionTypeId,
                async (Param p, WorkflowState state) =>
                {
                    _ = Task.Run(() => paramTcs.TrySetResult(p));
                    state.Scraps.Add(4);
                    await state.Save();
                }
            );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
            );

            paramTcs.Task.Result.ShouldBe(param);
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result.ResultType.ShouldBe(typeof(Unit).SimpleQualifiedName());
            storedFunction.State.DefaultDeserialize()
                .CastTo<WorkflowState>()
                .Scraps
                .ShouldBe(new[] {1, 2, 3, 4});
        }
    }

    private class WorkflowState : Domain.WorkflowState
    {
        public List<int> Scraps { get; set; } = new();
    }
    
    private record Param(string Id, int Value);
}