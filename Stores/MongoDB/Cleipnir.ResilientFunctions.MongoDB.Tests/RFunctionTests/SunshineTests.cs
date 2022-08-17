using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class SunshineTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SunshineTests
{
    [TestMethod]
    public override Task SunshineScenarioFunc() 
        => SunshineScenarioFunc(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SunshineScenarioFuncWithScrapbook() 
        => SunshineScenarioFuncWithScrapbook(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SunshineScenarioAction() 
        => SunshineScenarioAction(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SunshineScenarioActionWithScrapbook() 
        => SunshineScenarioActionWithScrapbook(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SunshineScenarioNullReturningFunc()
        => SunshineScenarioNullReturningFunc(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SunshineScenarioNullReturningFuncWithScrapbook()
        => SunshineScenarioNullReturningFuncWithScrapbook(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task InvocationModeShouldBeDirectInSunshineScenario()
        => InvocationModeShouldBeDirectInSunshineScenario(NoSql.AutoCreateAndInitializeStore());
}