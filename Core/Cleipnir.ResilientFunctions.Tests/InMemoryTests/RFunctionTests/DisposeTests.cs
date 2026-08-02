using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class DisposeTests
{
    [TestMethod]
    public async Task RegisteringFunctionOnDisposedRFunctionsThrowsException()
    {
        var rFunctions = await FunctionsRegistry.CreateAndStart(new InMemoryFunctionStore(), _ => { });
        rFunctions.Dispose();

        Should.Throw<ObjectDisposedException>(() =>
            _ = rFunctions.RegisterFunc(
                "id".ToFlowType(),
                (string _) => Succeed.WithUnit.ToTask()
            )
        );
    }
}