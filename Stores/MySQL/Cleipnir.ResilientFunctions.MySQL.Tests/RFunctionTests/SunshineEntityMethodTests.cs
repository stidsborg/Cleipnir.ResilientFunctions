using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.RFunctionTests;

[TestClass]
public class SunshineEntityMethodTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SunshineEntityMethodTests
{
    [TestMethod]
    public override Task SunshineScenarioFunc() => SunshineScenarioFunc(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task SunshineScenarioFuncWithScrapbook() 
        => SunshineScenarioFuncWithScrapbook(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task SunshineScenarioAction()
        => SunshineScenarioAction(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task SunshineScenarioActionWithScrapbook()
        => SunshineScenarioActionWithScrapbook(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task SunshineScenarioScheduleFunc()
        => SunshineScenarioScheduleFunc(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task SunshineScenarioScheduleFuncWithScrapbook()
        => SunshineScenarioScheduleFuncWithScrapbook(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task SunshineScenarioScheduleAction()
        => SunshineScenarioScheduleAction(Sql.AutoCreateAndInitializeStore());
    [TestMethod]
    public override Task SunshineScenarioScheduleActionWithScrapbook()
        => SunshineScenarioScheduleActionWithScrapbook(Sql.AutoCreateAndInitializeStore());
}