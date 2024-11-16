using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class LogStoreTests : TestTemplates.LogStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest() 
        => SunshineScenarioTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task GetEntriesWithOffsetTest()
        => GetEntriesWithOffsetTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task GetEntriesWithOffsetAndOwnerTest()
        => GetEntriesWithOffsetAndOwnerTest(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task AppendMultipleEntriesAtOnce()
        => AppendMultipleEntriesAtOnce(FunctionStoreFactory.Create());
}