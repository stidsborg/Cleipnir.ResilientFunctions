using System;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
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
        bool throwUnhandledException,
        [CallerMemberName] string callerMemberName = ""
    )
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create(callerMemberName);
        var (functionTypeId, functionInstanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.Zero,
                    postponedCheckFrequency: TimeSpan.Zero
                )
            );
            var rFunc = functionsRegistry
                .RegisterAction(
                    functionTypeId,
                    (string _) =>
                        throwUnhandledException
                            ? throw new Exception()
                            : Task.FromException(new Exception())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(async () => await rFunc(functionInstanceId.ToString(), PARAM));
            await BusyWait.Until(() => store.GetFunction(functionId).SelectAsync(sf => sf != null));
        }
        {
            var flag = new SyncedFlag();
            using var functionsRegistry = new FunctionsRegistry(
                store, 
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rFunc = functionsRegistry.RegisterFunc(
                functionTypeId,
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
            await Should.ThrowAsync<Exception>(async () => await rFunc(functionInstanceId.ToString(), PARAM));
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
        var functionTypeId = callerMemberName.ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(0),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(0)
                )
            );
            var nonCompletingFunctionsRegistry = functionsRegistry
                .RegisterAction(
                    functionTypeId,
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
                        postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                    )
                );
            var rAction = functionsRegistry.RegisterAction(functionTypeId,
                (string _) =>
                {
                    flag.Raise();
                    return Task.CompletedTask;
                }
            ).Invoke;
                
            await Task.Delay(250);
            flag.Position.ShouldBe(Lowered);
            
            var functionId = new FunctionId(functionTypeId, PARAM.ToFunctionInstanceId());
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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        {
            using var functionsRegistry = new FunctionsRegistry
            (
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(0),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(0)
                )
            );
            var nonCompletingFunctionsRegistry = functionsRegistry 
                .RegisterAction(
                    functionTypeId,
                    (string _) => Task.FromException(new Exception())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(nonCompletingFunctionsRegistry(functionInstanceId.ToString(), PARAM));
        }
        {
            var flag = new SyncedFlag();
            using var functionsRegistry = new FunctionsRegistry(
                store, 
                new Settings(
                    unhandledExceptionHandler.Catch,
                    leaseLength: TimeSpan.FromMilliseconds(100),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
                )
            );
            var rFunc = functionsRegistry
                .RegisterAction(
                    functionTypeId,
                    inner: (string _) => flag.Raise()
                )
                .Invoke;
            await Task.Delay(250);
            flag.Position.ShouldBe(Lowered);
            
            var status = await store.GetFunction(functionId).Map(t => t?.Status);
            status.ShouldNotBeNull();
            status.ShouldBe(Status.Failed);
            await Should.ThrowAsync<Exception>(rFunc(functionInstanceId.ToString(), PARAM));
        }
            
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }
    
    public abstract Task PassingInNullParameterResultsInArgumentNullException();
    public async Task PassingInNullParameterResultsInException(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ExceptionThrowingActionIsNotCompletedByWatchDog).ToFunctionTypeId();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        var flag = new SyncedFlag();
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.FromMilliseconds(2),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
            )
        );
        var rFunc = functionsRegistry
            .RegisterAction(
                functionTypeId,
                inner: (string _) => flag.Raise()
            )
            .Invoke;
        await Task.Delay(100);
        flag.Position.ShouldBe(Lowered);

        await Should.ThrowAsync<ArgumentNullException>(
            () => rFunc("someFunctionInstanceId", param: null!)
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
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        const string param = "test";
        {
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(unhandledExceptionHandler.Catch, leaseLength: TimeSpan.Zero)
            );
            var nonCompletingFunctionsRegistry = functionsRegistry 
                .RegisterAction(
                    functionTypeId,
                    (string _) => 
                        throwUnhandledException
                            ? throw new Exception()
                            : Task.FromException(new Exception())
                ).Invoke;

            await Should.ThrowAsync<Exception>(() => nonCompletingFunctionsRegistry(functionInstanceId.ToString(), param));
        }
        {
            var flag = new SyncedFlag();
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(unhandledExceptionHandler.Catch, leaseLength: TimeSpan.FromMilliseconds(100))
            );
            var rFunc = functionsRegistry.RegisterAction(
                functionTypeId,
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
            
            await Should.ThrowAsync<Exception>(() => rFunc(functionInstanceId.ToString(), param));
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
    
    public abstract Task FuncReturningTaskThrowsSerialization();
    public async Task FuncReturningTaskThrowsSerialization(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler.Catch,
                leaseLength: TimeSpan.FromMilliseconds(100),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );
        var funcRegistration = functionsRegistry
            .RegisterFunc(
                functionId.TypeId,
                inner: (string _) => Task.CompletedTask
            );

        var rFunc = funcRegistration.Invoke;
        
        await Should.ThrowAsync<SerializationException>(
            () => rFunc(functionId.InstanceId.Value, "test")
        );

        var controlPanel = await funcRegistration.ControlPanel(functionId.InstanceId).ShouldNotBeNullAsync();
        controlPanel.Status.ShouldBe(Status.Failed);
        
        unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
    }

    private class WorkflowState : Domain.WorkflowState { }
}