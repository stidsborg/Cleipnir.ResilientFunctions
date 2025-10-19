using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public abstract class WatchdogCompoundTests 
{
    public abstract Task FunctionCompoundTest();
    public async Task FunctionCompoundTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, _) = functionId;
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
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rFunc = functionsRegistry.RegisterFunc(
                flowType,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    return NeverCompletingTask.OfType<Result<string>>();
                }
            ).Invoke;

            _ = rFunc(functionId.Instance.Value, param);

            var actualParam = await paramTcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        {
            //second invocation is delayed
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();

            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterFunc(
                flowType,
                (Param p) =>
                {
                    Task.Run(() => paramTcs.TrySetResult(p));
                    return Postpone.Until(DateTime.UtcNow.AddMilliseconds(100)).ToResult<string>().ToTask();
                });
            
            await crashableStore.AfterPostponeFunctionFlag.WaitForRaised();
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
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterFunc(
                flowType,
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
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var registration = functionsRegistry.RegisterFunc(
                flowType,
                (Param p) => $"{p.Id}-{p.Value}".ToTask()
            );

            var storedId = registration.MapToStoredId(functionId.Instance);
            await BusyWait.Until(async () =>
                await store.GetFunction(storedId).Map(sf => sf!.Status) == Status.Succeeded
            );

            var storedFunction = await store.GetFunction(storedId);
            var results = await store.GetResults([storedId]);
            var resultBytes = results[storedId];
            resultBytes!.ToStringFromUtf8Bytes().DeserializeFromJsonTo<string>().CastTo<string>().ShouldBe($"{param.Id}-{param.Value}");
        }
    }

    public abstract Task ActionCompoundTest();
    public async Task ActionCompoundTest(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, _) = functionId;
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
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rAction = functionsRegistry
                .RegisterAction(
                    flowType,
                    inner: (Param p) =>
                    {
                        tcs.TrySetResult(p);
                        return NeverCompletingTask.OfVoidType;
                    })
                .Invoke;
            
            _ = rAction(functionId.Instance.Value, param);
            var actualParam = await tcs.Task;
            actualParam.ShouldBe(param);

            crashableStore.Crash();
        }
        //second invocation is delayed
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var paramTcs = new TaskCompletionSource<Param>();
            
            using var functionsRegistry = new FunctionsRegistry(
                crashableStore,
                new Settings(
                    unhandledExceptionCatcher.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry
                .RegisterAction(
                    flowType,
                    inner: (Param p) =>
                    {
                        Task.Run(() => paramTcs.TrySetResult(p));
                        return Postpone.Until(DateTime.UtcNow.AddMilliseconds(100)).ToUnitResult.ToTask();
                    }
                );

            await crashableStore.AfterPostponeFunctionFlag.WaitForRaised();
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
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            _ = functionsRegistry.RegisterAction(
                flowType,
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
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var registration = functionsRegistry
                .RegisterAction(
                    flowType,
                (Param p) => Task.Run(() => paramTcs.TrySetResult(p))
                );

            await BusyWait.Until(async () =>
                await store.GetFunction(registration.MapToStoredId(functionId.Instance)).Map(sf => sf!.Status) == Status.Succeeded
            );
            
            paramTcs.Task.Result.ShouldBe(param);
            
            await store.GetFunction(registration.MapToStoredId(functionId.Instance));
        }
    }
    
    public abstract Task RetentionWatchdogDeletesEligibleSucceededFunction();
    public async Task RetentionWatchdogDeletesEligibleSucceededFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, _) = functionId;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.FromMilliseconds(100),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(100),
                retentionPeriod: TimeSpan.FromMilliseconds(100)
            )
        );
        var registration = functionsRegistry.RegisterAction(
            flowType,
            inner: (string _, Workflow _) => Task.CompletedTask
        );

        await registration.Invoke(functionId.Instance.Value, "SomeParam");

        await BusyWait.Until(
            () => store.GetFunction(registration.MapToStoredId(functionId.Instance)).SelectAsync(sf => sf is not null)
        );
    }
    
    public abstract Task FlowIdIsCorrectWhenFlowIsStartedByWatchdog();
    public async Task FlowIdIsCorrectWhenFlowIsStartedByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var testId = TestFlowId.Create();
        var (flowType, _) = testId;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionCatcher.Catch,
                leaseLength: TimeSpan.FromMilliseconds(100),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(100),
                retentionPeriod: TimeSpan.FromMilliseconds(100)
            )
        );
        
        var registration = functionsRegistry.RegisterFunc(
            flowType,
            inner: async Task<FlowId> (string _, Workflow workflow) =>
            {
                await workflow.Messages.FirstOfType<string>(maxWait: TimeSpan.Zero);
                return workflow.FlowId;
            }
        );

        await registration.Schedule(testId.Instance.Value, param: "");
        var controlPanel = await registration.ControlPanel(testId.Instance).ShouldNotBeNullAsync();
        
        await BusyWait.Until(
            async () =>
            {
                await controlPanel.Refresh();
                return controlPanel.Status == Status.Suspended;
            }
        );

        await registration.SendMessage(testId.Instance, "some message");
        
        await BusyWait.Until(
            async () =>
            {
                await controlPanel.Refresh();
                return controlPanel.Status == Status.Succeeded;
            }
        );

        var flowId = controlPanel.Result;
        flowId.ShouldBe(testId);
        
        unhandledExceptionCatcher.ShouldNotHaveExceptions();
    }
    
    private record Param(string Id, int Value);
}