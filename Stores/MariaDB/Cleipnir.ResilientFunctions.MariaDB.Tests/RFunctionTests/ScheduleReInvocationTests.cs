namespace Cleipnir.ResilientFunctions.MariaDb.Tests.RFunctionTests;

[TestClass]
public class ScheduleReInvocationTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.ScheduleReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(FunctionStoreFactory.Create());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(FunctionStoreFactory.Create());
}