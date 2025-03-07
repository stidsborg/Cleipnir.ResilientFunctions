using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class ExceptionHandlingTests
{
    public abstract Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc();
    protected async Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc<string, string>( //explicit generic parameters to satisfy Rider-ide
            "typeId".ToFlowType(),
            Task<string> (string param) => throw new ArithmeticException("Division by zero")
        ).Invoke;
        
        await Should.ThrowAsync<FatalWorkflowException<ArithmeticException>>(async () => await rFunc("instanceId", "hello"));
        await Should.ThrowAsync<FatalWorkflowException<ArithmeticException>>(async () => await rFunc("instanceId", "hello"));
    }

    public abstract Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithState();
    protected async Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithState(Task<IFunctionStore> storeTask)
    {
        var store = new InMemoryFunctionStore();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry.RegisterFunc( 
            "typeId".ToFlowType(),
            Task<string> (string param) => throw new ArithmeticException("Division by zero")
        ).Invoke;
        
        await Should.ThrowAsync<FatalWorkflowException<ArithmeticException>>(async () => await rFunc("instanceId", "hello"));
        await Should.ThrowAsync<FatalWorkflowException<ArithmeticException>>(async () => await rFunc("instanceId", "hello"));
    }

    public abstract Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction();
    protected async Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction(Task<IFunctionStore> storeTask)
    {
        var store = new InMemoryFunctionStore();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry
            .RegisterAction(
                "typeId".ToFlowType(),
                Task (string _) => throw new ArithmeticException("Division by zero")
            )
            .Invoke;

        await Should.ThrowAsync<FatalWorkflowException<ArithmeticException>>(async () => await rFunc("instanceId", "hello"));
        await Should.ThrowAsync<FatalWorkflowException<ArithmeticException>>(async () => await rFunc("instanceId", "hello"));
    }

    public abstract Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithState();
    protected async Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithState(Task<IFunctionStore> storeTask)
    {
        var store = new InMemoryFunctionStore();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = functionsRegistry
            .RegisterAction(
                "typeId".ToFlowType(),
                Task (string _) => throw new ArithmeticException("Division by zero")
            )
            .Invoke;

        await Should.ThrowAsync<FatalWorkflowException<ArithmeticException>>(async () => await rFunc("instanceId", "hello"));
        await Should.ThrowAsync<FatalWorkflowException<ArithmeticException>>(async () => await rFunc("instanceId", "hello"));
    }
}