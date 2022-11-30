using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class SuspensionTests
{
    public abstract Task ActionCanBeSuspended();
    protected async Task ActionCanBeSuspended(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(ActionCanBeSuspended).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            Result(string _) => Suspend.Until(1)
        );

        _ = rAction.Invoke(functionInstanceId, "hello world");

        await BusyWait.Until(
            () => store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Suspended)
        );

        var sf = await store
            .GetFunction(functionId)
            .SelectAsync(sf => sf?.SuspendedUntilEventSourceCount);

        sf.ShouldNotBeNull();
        sf.Value.ShouldBe(1);
    }
    
    public abstract Task FunctionCanBeSuspended();
    protected async Task FunctionCanBeSuspended(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionTypeId = nameof(FunctionCanBeSuspended).ToFunctionTypeId();
        var functionInstanceId = "functionInstanceId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = new RFunctions
        (
            store,
            new Settings(unhandledExceptionHandler.Catch)
        );

        var rAction = rFunctions.RegisterFunc(
            functionTypeId,
            Result<string>(string _) => Suspend.Until(1)
        );

        _ = rAction.Invoke(functionInstanceId, "hello world");

        await BusyWait.Until(
            () => store.GetFunction(functionId).SelectAsync(sf => sf?.Status == Status.Suspended)
        );

        var sf = await store
            .GetFunction(functionId)
            .SelectAsync(sf => sf?.SuspendedUntilEventSourceCount);

        sf.ShouldNotBeNull();
        sf.Value.ShouldBe(1);
    }
}