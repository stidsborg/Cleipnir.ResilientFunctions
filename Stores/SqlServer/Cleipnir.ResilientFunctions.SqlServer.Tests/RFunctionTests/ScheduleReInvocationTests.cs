using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class ScheduleReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ScheduleReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task ActionWithStateReInvocationSunshineScenario()
        => ActionWithStateReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task FuncWithStateReInvocationSunshineScenario()
        => FuncWithStateReInvocationSunshineScenario(FunctionStoreFactory.Create());
}