using System;
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
    public void RegisteringFunctionOnDisposedRFunctionsThrowsException()
    {
        var rFunctions = RFunctions.Create(new InMemoryFunctionStore());
        rFunctions.Dispose();

        Should.Throw<ObjectDisposedException>(() =>
            _ = rFunctions.Register(
                "id".ToFunctionTypeId(),
                (string _) => Succeed.WithoutValue.ToTask()
            )
        );
    }
}