using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class ReInvocationTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterIsPassedInOnReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterAndStateIsPassedInOnReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionDoesNotExist()
        => ReInvocationFailsWhenTheFunctionDoesNotExist(FunctionStoreFactory.Create());
}