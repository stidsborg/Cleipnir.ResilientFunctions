using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class ScrapbookTests : ResilientFunctions.Tests.TestTemplates.ScrapbookTests
{
    [TestMethod]
    public override Task SunshineScenario()
        => SunshineScenario(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override async Task ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected()
        => await ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration()
        => ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task WhenConcreteScrapbookTypeIsNotSubtypeOfScrapbookAnExceptionIsThrownAtRegistration()
        => WhenConcreteScrapbookTypeIsNotSubtypeOfScrapbookAnExceptionIsThrownAtRegistration(Sql.AutoCreateAndInitializeStore());
}