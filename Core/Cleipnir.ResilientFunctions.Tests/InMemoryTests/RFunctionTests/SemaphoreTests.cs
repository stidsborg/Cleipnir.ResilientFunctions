using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class SemaphoreTests : TestTemplates.FunctionTests.SemaphoreTests
{
    [TestMethod]
    public override Task SunshineTest()
        => SunshineTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WaitingFlowIsInterruptedAfterSemaphoreBecomesFree()
        => WaitingFlowIsInterruptedAfterSemaphoreBecomesFree(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SemaphoreAllowsTwoFlowsToContinueAtTheSameTime()
        => SemaphoreAllowsTwoFlowsToContinueAtTheSameTime(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingSemaphoreCanBeForceReleased()
        => ExistingSemaphoreCanBeForceReleased(FunctionStoreFactory.Create());
}