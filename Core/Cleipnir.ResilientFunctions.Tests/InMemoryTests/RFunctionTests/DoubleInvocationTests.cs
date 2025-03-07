using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class DoubleInvocationTests : TestTemplates.FunctionTests.DoubleInvocationTests
{
    [TestMethod]
    public override Task SecondInvocationWaitsForAndReturnsSuccessfulResult()
        => SecondInvocationWaitsForAndReturnsSuccessfulResult(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondInvocationFailsOnSuspendedFlow()
        => SecondInvocationFailsOnSuspendedFlow(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondInvocationFailsOnPostponedFlow()
        => SecondInvocationFailsOnPostponedFlow(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SecondInvocationFailsOnFailedFlow()
        => SecondInvocationFailsOnFailedFlow(FunctionStoreFactory.Create());
}