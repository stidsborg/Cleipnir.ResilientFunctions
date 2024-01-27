using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class ExceptionHandlingTests
{
    public abstract Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc();
    protected async Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFunc(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = rFunctions.RegisterFunc<string, string>( //explicit generic parameters to satisfy Rider-ide
            "typeId".ToFunctionTypeId(),
            string (string param) => throw new ArithmeticException("Division by zero")
        ).Invoke;
        
        await Should.ThrowAsync<ArithmeticException>(async () => await rFunc("instanceId", "hello"));
        await Should.ThrowAsync<PreviousFunctionInvocationException>(async () => await rFunc("instanceId", "hello"));
    }

    public abstract Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook();
    protected async Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnFuncWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var store = new InMemoryFunctionStore();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = rFunctions.RegisterFunc<string, ListScrapbook<string>, string>( //explicit generic parameters to satisfy Rider-ide
            "typeId".ToFunctionTypeId(),
            string (string param, ListScrapbook<string> scrapbook) => throw new ArithmeticException("Division by zero")
        ).Invoke;
        
        await Should.ThrowAsync<ArithmeticException>(async () => await rFunc("instanceId", "hello"));
        await Should.ThrowAsync<PreviousFunctionInvocationException>(async () => await rFunc("instanceId", "hello"));
    }

    public abstract Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction();
    protected async Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnAction(Task<IFunctionStore> storeTask)
    {
        var store = new InMemoryFunctionStore();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = rFunctions
            .RegisterAction(
                "typeId".ToFunctionTypeId(),
                void (string _) => throw new ArithmeticException("Division by zero")
            )
            .Invoke;
        
        await Should.ThrowAsync<ArithmeticException>(async () => await rFunc("instanceId", "hello"));
        await Should.ThrowAsync<PreviousFunctionInvocationException>(async () => await rFunc("instanceId", "hello"));
    }

    public abstract Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook();
    protected async Task UnhandledExceptionIsRethrownWhenEnsuringSuccessOnActionWithScrapbook(Task<IFunctionStore> storeTask)
    {
        var store = new InMemoryFunctionStore();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        using var rFunctions = new FunctionsRegistry(store, new Settings(unhandledExceptionCatcher.Catch));

        var rFunc = rFunctions
            .RegisterAction(
                "typeId".ToFunctionTypeId(),
                void (string _) => throw new ArithmeticException("Division by zero")
            )
            .Invoke;

        await Should.ThrowAsync<ArithmeticException>(async () => await rFunc("instanceId", "hello"));
        await Should.ThrowAsync<PreviousFunctionInvocationException>(async () => await rFunc("instanceId", "hello"));
    }
}