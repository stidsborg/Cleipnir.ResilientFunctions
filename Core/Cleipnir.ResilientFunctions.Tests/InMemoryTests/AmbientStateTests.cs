using System;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class AmbientStateTests
{
    [TestMethod]
    public void ExceptionIsThrownWhenFetchingFlowIdOutsideOfFrameworkExecution()
    {
        Should.Throw<InvalidOperationException>(() => CurrentFlow.StoredId);
    }
}