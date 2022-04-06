using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class InMemoryDelayTest
{
    private FunctionId FunctionId { get; } = new("FunctionTypeId", "FunctionInstanceId");

    public abstract Task NonExpiredInMemoryDelayedInvocationIsNotPickedUpByWatchdogDespiteCrash();
    protected async Task NonExpiredInMemoryDelayedInvocationIsNotPickedUpByWatchdogDespiteCrash(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var disposables = new CombinableDisposable();
        var fallbackInvoked = new SyncedFlag();
        //set up invoking rfunctions instance and call
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var rFunctions = new RFunctions(crashableStore);
            
            var rFunc = rFunctions
                .Func(FunctionId.TypeId,
                    async Task<string>(string param) =>
                    {
                        await Task.CompletedTask;
                        return param.ToUpper();
                    }
                ).WithPostInvoke(
                    (_, _) => Postpone.For(TimeSpan.FromMinutes(1), inProcessWait: true)
                ).Register()
                .Invoke;
            
            _ = rFunc(FunctionId.InstanceId.Value, "hello world");
            
            await Task.Delay(100);
            crashableStore.Crash();
        }

        //set up fallback rfunctions instance
        {
            var rFunctions = new RFunctions(store, crashedCheckFrequency: TimeSpan.FromMilliseconds(5));
            disposables.Add(rFunctions);
            rFunctions
                .Func(FunctionId.TypeId,
                    inner: async Task<string>(string param) =>
                    {
                        await Task.CompletedTask;
                        fallbackInvoked.Raise();
                        return param.ToUpper();
                    }
                )
                .Register();
        }
        
        await Task.Delay(100);
        fallbackInvoked.Position.ShouldBe(FlagPosition.Lowered);
    }

    public abstract Task ExpiredInMemoryDelayedInvocationIsPickedUpByWatchdogOnCrash();
    protected async Task ExpiredInMemoryDelayedInvocationIsPickedUpByWatchdogOnCrash(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var disposables = new CombinableDisposable();
        var fallbackInvoked = new SyncedFlag();
        //set up fallback rfunctions instance
        {
            var rFunctions = new RFunctions(store, crashedCheckFrequency: TimeSpan.FromMilliseconds(50));
            disposables.Add(rFunctions);
            rFunctions
                .Func(FunctionId.TypeId,
                    inner: async Task<string>(string param) =>
                    {
                        await Task.CompletedTask;
                        fallbackInvoked.Raise();
                        return param.ToUpper();
                    }
                )
                .Register();
        }
        //set up invoking rfunctions instance and call
        {
            var crashableStore = new CrashableFunctionStore(store);
            using var rFunctions = new RFunctions(crashableStore);
            
            var rFunc = rFunctions
                .Func(FunctionId.TypeId,
                    async Task<string>(string param) =>
                    {
                        await Task.CompletedTask;
                        return param.ToUpper();
                    }
                ).WithPostInvoke(
                    (_, _) => Postpone.For(TimeSpan.FromMilliseconds(10), inProcessWait: true)
                ).Register()
                .Invoke;
            
            _ = rFunc(FunctionId.InstanceId.Value, "hello world");
            
            await Task.Delay(100);
            crashableStore.Crash();
        }

        await Task.Delay(500);
        fallbackInvoked.Position.ShouldBe(FlagPosition.Raised);
    }
}