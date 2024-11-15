using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests;

[TestClass]
public class LogStoreTests : ResilientFunctions.Tests.TestTemplates.LogStoreTests
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
}