using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using static Cleipnir.ResilientFunctions.Tests.InMemoryTests.Utils;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class SunshineEntityMethodTests : TestTemplates.RFunctionTests.SunshineEntityMethodTests
{
    [TestMethod]
    public override Task SunshineScenarioFunc() => SunshineScenarioFunc(CreateInMemoryFunctionStoreTask());
    [TestMethod]
    public override Task SunshineScenarioFuncWithScrapbook() 
        => SunshineScenarioFuncWithScrapbook(CreateInMemoryFunctionStoreTask());
    [TestMethod]
    public override Task SunshineScenarioAction()
        => SunshineScenarioAction(CreateInMemoryFunctionStoreTask());
    [TestMethod]
    public override Task SunshineScenarioActionWithScrapbook()
        => SunshineScenarioActionWithScrapbook(CreateInMemoryFunctionStoreTask());
    [TestMethod]
    public override Task SunshineScenarioScheduleFunc()
        => SunshineScenarioScheduleFunc(CreateInMemoryFunctionStoreTask());
    [TestMethod]
    public override Task SunshineScenarioScheduleFuncWithScrapbook()
        => SunshineScenarioScheduleFuncWithScrapbook(CreateInMemoryFunctionStoreTask());
    [TestMethod]
    public override Task SunshineScenarioScheduleAction()
        => SunshineScenarioScheduleAction(CreateInMemoryFunctionStoreTask());
    [TestMethod]
    public override Task SunshineScenarioScheduleActionWithScrapbook()
        => SunshineScenarioScheduleActionWithScrapbook(CreateInMemoryFunctionStoreTask());
}