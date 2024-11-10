using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests;

[TestClass]
public class StatesStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.StatesStoreTests
{
    [TestMethod]
    public override Task SunshineScenario()
        => SunshineScenario(FunctionStoreFactory.Create());
}