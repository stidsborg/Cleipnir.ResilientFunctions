using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class DoubleInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.DoubleInvocationTests
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