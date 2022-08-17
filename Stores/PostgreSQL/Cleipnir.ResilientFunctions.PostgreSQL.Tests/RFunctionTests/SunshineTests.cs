using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class SunshineTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SunshineTests
{
    [TestMethod]
    public override Task SunshineScenarioFunc() 
        => SunshineScenarioFunc(Sql.AutoCreateAndInitializeStore());

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
    public override Task SunshineScenarioNullReturningFunc()
        => SunshineScenarioNullReturningFunc(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SunshineScenarioNullReturningFuncWithScrapbook()
        => SunshineScenarioNullReturningFuncWithScrapbook(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task InvocationModeShouldBeDirectInSunshineScenario()
        => InvocationModeShouldBeDirectInSunshineScenario(Sql.AutoCreateAndInitializeStore());
}