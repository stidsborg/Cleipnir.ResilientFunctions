using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

[TestClass]
public class ReInvocationTests
{
    [TestMethod]
    public async Task FailedRActionCanBeReInvoked()
    {
        var store = new InMemoryFunctionStore();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        
        var functionId = new FunctionId("functionType", "functionInstance");
        var parameter = "functionInstance";
        await store.CreateFunction(
            functionId,
            new StoredParameter(parameter.ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        ).ShouldBeTrueAsync();

        await store.SetFunctionState(
            functionId,
            Status.Failed,
            scrapbookJson: null,
            result: null,
            failed: new StoredFailure(new Exception("").ToJson(), typeof(Exception).SimpleQualifiedName()),
            postponedUntil: null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var rFunctions = RFunctions.Create(store, unhandledExceptionCatcher.Catch);

        var rAction = rFunctions.Register(
            functionId.TypeId,
            (string _) => 
                RResult.Success.ToTask(),
            _ => _
        );

        await rAction(parameter, reInvoke: true);

        await store.GetFunction(functionId).Map(sf => sf?.Status).ShouldBeAsync(Status.Succeeded);
        
        unhandledExceptionCatcher.ThrownExceptions.ShouldBeEmpty();
    }
}