using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests;

[TestClass]
public class ScrapbookTests : ResilientFunctions.Tests.TestTemplates.ScrapbookTests
{
    [TestMethod]
    public override Task SunshineScenario()
        => SunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override async Task ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected()
        => await ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration()
        => ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ChangesToStateDictionaryArePersisted()
        => ChangesToStateDictionaryArePersisted(FunctionStoreFactory.Create());
}