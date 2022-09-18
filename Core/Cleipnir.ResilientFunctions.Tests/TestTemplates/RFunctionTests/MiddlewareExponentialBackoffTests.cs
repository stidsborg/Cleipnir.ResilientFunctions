using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Middlewares;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class MiddlewareExponentialBackoffTests
{
    public abstract Task UnhandledExceptionResultsInPostponedFunc();
    protected async Task UnhandledExceptionResultsInPostponedFunc(Task<IFunctionStore> storeTask)
    {
        var functionType = "SomeFunctionType".ToFunctionTypeId();
        var store = await storeTask;
        var syncedCounter = new SyncedCounter();
        var middleware = new ExponentialBackoffMiddleware(
            firstDelay: TimeSpan.FromMilliseconds(100),
            factor: 2,
            maxTries: 4,
            inMemoryThreshold: TimeSpan.FromMilliseconds(500)
        );
        
        using var rFunctions = new RFunctions(
            store,
            new Settings().UseMiddleware(middleware)
        );
        
        var rFunc = rFunctions.RegisterFunc<string, string>(
            functionType,
            string (_) =>
            {
                syncedCounter.Increment();
                throw new Exception("oh no");
            }).Invoke;

        FunctionInvocationPostponedException? thrownException = null;
        try
        {
            await rFunc.Invoke("1", "1");
        }
        catch (FunctionInvocationPostponedException exception)
        {
            thrownException = exception;
        }
        thrownException.ShouldNotBeNull();

        await BusyWait.Until(() => store
            .GetFunction(new FunctionId(functionType, "1"))
            .Map(sf => sf?.Status == Status.Failed)
        );
        
        syncedCounter.Current.ShouldBe(4);
    }
}