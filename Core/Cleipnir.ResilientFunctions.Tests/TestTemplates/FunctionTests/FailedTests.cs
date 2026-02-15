using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.FlagPosition;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class FailedTests
{
    private const string Param = "test";
    
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
            var actionRegistration = functionsRegistry
                .RegisterAction(
                    flowType,
                    (string _) =>
                        throwUnhandledException
                            ? throw new InvalidOperationException()
                            : Task.FromException(new InvalidOperationException())
                );

            await Should.ThrowAsync<FatalWorkflowException>(async () => await actionRegistration.Invoke(flowInstance.ToString(), Param));
            var sf = await store.GetFunction(actionRegistration.MapToStoredId(functionId.Instance));
            sf.ShouldNotBeNull();
        }
        {
            var flag = new SyncedFlag();
            using var functionsRegistry = new FunctionsRegistry(
                store,
                new Settings(
                    unhandledExceptionHandler.Catch,
                    watchdogCheckFrequency: TimeSpan.FromMilliseconds(1_000)
                )
            );
            var rFunc = functionsRegistry.RegisterFunc(
                flowType,
                (string s) =>
                {
                    flag.Raise();
                    return s.ToUpper().ToTask();
                }
            );
            await Task.Delay(250);

            flag.Position.ShouldBe(Lowered);

            var sf = await store.GetFunction(rFunc.MapToStoredId(functionId.Instance));
            sf.ShouldNotBeNull();
            sf.Status.ShouldBe(Status.Failed);
            await Should.ThrowAsync<Exception>(async () => await rFunc.Invoke(flowInstance.ToString(), Param));
        }

        var fwe = (FatalWorkflowException) unhandledExceptionHandler.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
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
                            ? throw new InvalidOperationException()
                            : Task.FromException(new InvalidOperationException())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(() => nonCompletingFunctionsRegistry(Param, Param));
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
            );

            await Task.Delay(250);
            flag.Position.ShouldBe(Lowered);

            var functionId = new FlowId(flowType, Param.ToFlowInstance());
            var storedFunction = await store.GetFunction(rAction.MapToStoredId(functionId.Instance));
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);

            await Should.ThrowAsync<Exception>(() => rAction.Invoke(Param, Param));
        }

        var fwe = (FatalWorkflowException) unhandledExceptionHandler.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
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
                    (string _) => Task.FromException(new InvalidOperationException())
                )
                .Invoke;

            await Should.ThrowAsync<Exception>(nonCompletingFunctionsRegistry(flowInstance.ToString(), Param));
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
                    });
            await Task.Delay(250);
            flag.Position.ShouldBe(Lowered);

            var status = await store.GetFunction(rFunc.MapToStoredId(functionId.Instance)).Map(t => t?.Status);
            status.ShouldNotBeNull();
            status.ShouldBe(Status.Failed);
            await Should.ThrowAsync<Exception>(rFunc.Invoke(flowInstance.ToString(), Param));
        }

        var fwe = (FatalWorkflowException) unhandledExceptionHandler.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
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
        var registration = functionsRegistry
            .RegisterAction(
                flowType,
                inner: (string _) =>
                {
                    flag.Raise();
                    return Task.CompletedTask;
                });
        await Task.Delay(100);
        flag.Position.ShouldBe(Lowered);

        await Should.ThrowAsync<ArgumentNullException>(
            () => registration.Invoke("someflowInstance", param: null!)
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
                            ? throw new InvalidOperationException()
                            : Task.FromException(new InvalidOperationException())
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
                    return Succeed.WithUnit.ToTask();
                }
            );

            await Task.Delay(250);
            flag.Position.ShouldBe(Lowered);

            var storedFunction = await store.GetFunction(rFunc.MapToStoredId(functionId.Instance));
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Failed);

            await Should.ThrowAsync<Exception>(() => rFunc.Invoke(flowInstance.ToString(), param));
        }

        var fwe = (FatalWorkflowException) unhandledExceptionHandler.ThrownExceptions.Single().InnerException!;
        fwe.ErrorType.ShouldBe(typeof(InvalidOperationException));
        if (throwUnhandledException)
        {
            var key = (await store.TypeStore.GetAllFlowTypes()).Values.First();
            await store
                .GetFunction(StoredId.Create(key, functionId.Instance.Value))
                .Map(f => f?.Exception)
                .ShouldNotBeNullAsync();
        }
    }
}