using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

[TestClass]
public class ExceptionHandlingTests
{
    [TestMethod]
    public async Task UnhandledExceptionIsCaught()
    {
        var store = new InMemoryFunctionStore();
        var unhandledExceptionCatcher = new UnhandledExceptionCatcher();
        var rFunctions = new RFunctions(store, unhandledExceptionCatcher.Catch);

        var rFunc = rFunctions.Register<string, string>(
            "typeId".ToFunctionTypeId(),
            param => throw new ArithmeticException("Division by zero")
        ).Invoke;

        var result = await rFunc("instanceId", "hello");

        Should.Throw<ArithmeticException>(result.EnsureSuccess);
    }
}