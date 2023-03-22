using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests.RFunctionTests;

[TestClass]
public class SunshineEntityMethodTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SunshineEntityMethodTests
{
    [TestMethod]
    public override Task SunshineScenarioFunc() => SunshineScenarioFunc(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task SunshineScenarioFuncWithScrapbook() 
        => SunshineScenarioFuncWithScrapbook(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task SunshineScenarioAction()
        => SunshineScenarioAction(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task SunshineScenarioActionWithScrapbook()
        => SunshineScenarioActionWithScrapbook(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task SunshineScenarioScheduleFunc()
        => SunshineScenarioScheduleFunc(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task SunshineScenarioScheduleFuncWithScrapbook()
        => SunshineScenarioScheduleFuncWithScrapbook(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task SunshineScenarioScheduleAction()
        => SunshineScenarioScheduleAction(FunctionStoreFactory.FunctionStoreTask);
    [TestMethod]
    public override Task SunshineScenarioScheduleActionWithScrapbook()
        => SunshineScenarioScheduleActionWithScrapbook(FunctionStoreFactory.FunctionStoreTask);
}