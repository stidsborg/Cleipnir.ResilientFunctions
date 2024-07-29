using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.FlagPosition;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class FailedTests
{
    private const string PARAM = "test";
    
    public abstract Task ExceptionThrowingFuncIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingFuncIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(storeTask, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingFuncIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingFuncIsNotCompletedByWatchDog(storeTask, throwUnhandledException: true);
    
    private async Task ExceptionThrowingFuncIsNotCompletedByWatchDog(
        Task<IFunctionStore> storeTask, 
        bool throwUnhandledException
    )
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.Zero,
                    enableWatchdogs: false
                )
            );
            var rFunc = functionsRegistry
                .RegisterAction(
                    flowType,
                    (string _) =>
                        throwUnhandledException
                            ? throw new Exception()
                            : Task.FromException(new Exception())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(async () => await rFunc(flowInstance.ToString(), PARAM));
            var sf = await store.GetFunction(functionId);
            sf.ShouldNotBeNull();
        }
        {
            var flag = new SyncedFlag();
            using var functionsRegistry = new FunctionsRegistry(
                store, 
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rFunc = functionsRegistry.RegisterFunc(
                flowType,
                (string s) =>
                {
                    flag.Raise();
                    return s.ToUpper().ToTask();
                }
            ).Invoke;
            await Task.Delay(250);
            
            flag.Position.ShouldBe(Lowered);
            
            var sf = await store.GetFunction(functionId);
            sf.ShouldNotBeNull();
            sf.Status.ShouldBe(Status.Failed);
            await Should.ThrowAsync<Exception>(async () => await rFunc(flowInstance.ToString(), PARAM));
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ExceptionThrowingFuncWithStateIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingFuncWithStateIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingFuncWithStateIsNotCompletedByWatchDog(storeTask, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingFuncWithStateIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingFuncWithStateIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask) 
        => ExceptionThrowingFuncWithStateIsNotCompletedByWatchDog(storeTask, throwUnhandledException: true);
    
    private async Task ExceptionThrowingFuncWithStateIsNotCompletedByWatchDog(
        Task<IFunctionStore> storeTask,
        bool throwUnhandledException, 
        [CallerMemberName] string callerMemberName = ""
    )
    {
        var store = await storeTask;
        var flowType = callerMemberName.ToFlowType();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(0),
                    enableWatchdogs: false
                )
            );
            var nonCompletingFunctionsRegistry = functionsRegistry
                .RegisterAction(
                    flowType,
                    (string _) =>
                        throwUnhandledException 
                            ? throw new Exception()
                            : Task.FromException(new Exception())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(() => nonCompletingFunctionsRegistry(PARAM, PARAM));
        }
        {
            var flag = new SyncedFlag();
            using var functionsRegistry = new FunctionsRegistry(
                    store,
                    new Settings(
                        unhandledExceptionHandler.Catch,
                        leaseLength: TimeSpan.FromMilliseconds(100),
                        watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                    )
                );
            var rAction = functionsRegistry.RegisterAction(flowType,
                (string _) =>
                {
                    flag.Raise();
                    return Task.CompletedTask;
                }
            ).Invoke;
                
            await Task.Delay(250);
            flag.Position.ShouldBe(Lowered);
            
            var functionId = new FlowId(flowType, PARAM.ToFlowInstance());
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);

            await Should.ThrowAsync<Exception>(() => rAction(PARAM, PARAM));
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task ExceptionThrowingActionIsNotCompletedByWatchDog();
    public async Task ExceptionThrowingActionIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(0),
                    enableWatchdogs: false
                )
            );
            var nonCompletingFunctionsRegistry = functionsRegistry 
                .RegisterAction(
                    flowType,
                    (string _) => Task.FromException(new Exception())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(nonCompletingFunctionsRegistry(flowInstance.ToString(), PARAM));
        }
        {
            var flag = new SyncedFlag();
            using var functionsRegistry = new FunctionsRegistry(
                store, 
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rFunc = functionsRegistry
                .RegisterAction(
                    flowType,
                    inner: (string _) =>
                    {
                        flag.Raise();
                        return Task.CompletedTask;
                    })
                .Invoke;
            await Task.Delay(250);
            flag.Position.ShouldBe(Lowered);
            
            var status = await store.GetFunction(functionId).Map(t => t?.Status);
            status.ShouldNotBeNull();
            status.ShouldBe(Status.Failed);
            await Should.ThrowAsync<Exception>(rFunc(flowInstance.ToString(), PARAM));
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task PassingInNullParameterResultsInArgumentNullException();
    public async Task PassingInNullParameterResultsInException(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var flowType = nameof(ExceptionThrowingActionIsNotCompletedByWatchDog).ToFlowType();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var flag = new SyncedFlag();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.FromMilliseconds(2),
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(2)
            )
        );
        var rFunc = functionsRegistry
            .RegisterAction(
                flowType,
                inner: (string _) =>
                {
                    flag.Raise();
                    return Task.CompletedTask;
                })
            .Invoke;
        await Task.Delay(100);
        flag.Position.ShouldBe(Lowered);

        await Should.ThrowAsync<ArgumentNullException>(
            () => rFunc("someflowInstance", param: null!)
        );
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }

    public abstract Task ExceptionThrowingActionWithStateIsNotCompletedByWatchDog();
    protected Task ExceptionThrowingActionWithStateIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingActionWithStateIsNotCompletedByWatchDog(storeTask, throwUnhandledException: false);
    public abstract Task UnhandledExceptionThrowingActionWithStateIsNotCompletedByWatchDog();
    protected Task UnhandledExceptionThrowingActionWithStateIsNotCompletedByWatchDog(Task<IFunctionStore> storeTask)
        => ExceptionThrowingActionWithStateIsNotCompletedByWatchDog(storeTask, throwUnhandledException: true);
    
    private async Task ExceptionThrowingActionWithStateIsNotCompletedByWatchDog(
        Task<IFunctionStore> storeTask,
        bool throwUnhandledException,
        [CallerMemberName] string callerMemberName = ""
    )
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(unhandledExceptionHandler.Catch, leaseLength: TimeSpan.Zero, enableWatchdogs: false)
            );
            var nonCompletingFunctionsRegistry = functionsRegistry 
                .RegisterAction(
                    flowType,
                    (string _) => 
                        throwUnhandledException
                            ? throw new Exception()
                            : Task.FromException(new Exception())
                ).Invoke;

            await Should.ThrowAsync<Exception>(() => nonCompletingFunctionsRegistry(flowInstance.ToString(), param));
        }
        {
            var flag = new SyncedFlag();
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(unhandledExceptionHandler.Catch, leaseLength: TimeSpan.FromMilliseconds(100))
            );
            var rFunc = functionsRegistry.RegisterAction(
                flowType,
                (string _) =>
                {
                    flag.Raise();
                    return Succeed.WithoutValue.ToTask();
                }
            ).Invoke;
                
            await Task.Delay(250);
            flag.Position.ShouldBe(Lowered);
            
            var storedFunction = await store.GetFunction(functionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);
            
            await Should.ThrowAsync<Exception>(() => rFunc(flowInstance.ToString(), param));
        }
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
        if (throwUnhandledException)
        {
            await store
                .GetFunction(functionId)
                .Map(f => f?.Exception)
                .ShouldNotBeNullAsync();
        }
    }
    
    private class FlowState : Domain.FlowState { }
}