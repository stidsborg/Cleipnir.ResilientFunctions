using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils.Scrapbooks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using OnFailure = Cleipnir.ResilientFunctions.InnerAdapters.OnFailure;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class UnhandledFuncExceptionExponentialBackoffTests
{
    public abstract Task UnhandledExceptionResultsInPostponedFunc();
    protected async Task UnhandledExceptionResultsInPostponedFunc(Task<IFunctionStore> storeTask)
    {
        var functionType = "SomeFunctionType".ToFunctionTypeId();
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var syncedException = new Synced<Exception>();
        var rFunc = rFunctions.RegisterFunc(
            functionType,
            OnFailure.BackoffExponentially<string, BackoffScrapbook, string>(
                string (string _, BackoffScrapbook _) => 
                    throw new Exception("oh no"),
                firstDelay: TimeSpan.FromMilliseconds(100),
                factor: 2,
                maxRetries: 3,
                onException: (exception, _) => syncedException.Value = exception
            )
        );

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
    }
}