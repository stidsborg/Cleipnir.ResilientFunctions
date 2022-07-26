using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests;

[TestClass]
public class ScrapbookTests : ResilientFunctions.Tests.TestTemplates.ScrapbookTests
{
    [TestMethod]
    public override Task SunshineScenario()
        => SunshineScenario(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override async Task ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected()
        => await ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration()
        => ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task WhenConcreteScrapbookTypeIsNotSubtypeOfScrapbookAnExceptionIsThrownAtRegistration()
        => WhenConcreteScrapbookTypeIsNotSubtypeOfScrapbookAnExceptionIsThrownAtRegistration(NoSql.AutoCreateAndInitializeStore());
}