using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class DoubleInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.DoubleInvocationTests
{
    [TestMethod]
    public override Task SecondInvocationWaitsForAndReturnsSuccessfulResult()
        => SecondInvocationWaitsForAndReturnsSuccessfulResult(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SecondInvocationFailsOnSuspendedFlow()
        => SecondInvocationFailsOnSuspendedFlow(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SecondInvocationFailsOnPostponedFlow()
        => SecondInvocationFailsOnPostponedFlow(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SecondInvocationFailsOnFailedFlow()
        => SecondInvocationFailsOnFailedFlow(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
}