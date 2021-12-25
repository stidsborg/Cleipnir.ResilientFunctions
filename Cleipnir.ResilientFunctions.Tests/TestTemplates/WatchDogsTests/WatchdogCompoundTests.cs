using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

[TestClass]
public class WatchdogCompoundTests //todo make this into a test template
{
    private static Task<RResult<string>> NeverCompletingTask => new(() => default!);
    
    [TestMethod]
    public async Task FunctionCompoundTest()
    {
        var store = new InMemoryFunctionStore();
        var functionTypeId = nameof(FunctionCompoundTest).ToFunctionTypeId();
        const string functionInstanceId = "SomeId";
        var functionId = new FunctionId(functionTypeId, functionInstanceId);

        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        {
            var crashableStore = store.ToCrashableFunctionStore();
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch
            );
            var rFunc = rFunctions.Register(
                functionTypeId,
                (Param _) => NeverCompletingTask,
                param => param.Id
            );
            var onPersisted = new TaskCompletionSource();
            _ = rFunc(new Param(functionInstanceId, 25), onPersisted: onPersisted.SetResult);
            await onPersisted.Task;
            
            crashableStore.Crash();
        }
        {
            var crashableStore = store.ToCrashableFunctionStore();
            var afterNextSetFunctionState = crashableStore.AfterSetFunctionState;
            using var rFunctions = RFunctions.Create(
                crashableStore,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param _) => TimeSpan.FromMilliseconds(10).ToPostponedRResult<string>().ToTask(),
                param => param.Id
            );

            await afterNextSetFunctionState;
            crashableStore.Crash();
        }
        {
            using var rFunctions = RFunctions.Create(
                store,
                unhandledExceptionCatcher.Catch,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(1),
                postponedCheckFrequency: TimeSpan.FromMilliseconds(1)
            );
            _ = rFunctions.Register(
                functionTypeId,
                (Param _) => "hello".ToSucceededRResult().ToTask(),
                param => param.Id
            );

            await BusyWait.Until(async () =>
                await store.GetFunction(functionId).Map(sf => sf!.Status) == Status.Succeeded
           );

            var storedFunction = await store.GetFunction(functionId);
            storedFunction!.Result!.Deserialize().CastTo<string>().ShouldBe("hello");
        }
    }

    private record Param(string Id, int Value);
}