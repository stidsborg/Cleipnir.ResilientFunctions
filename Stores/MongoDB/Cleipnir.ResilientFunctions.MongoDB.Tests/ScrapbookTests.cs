using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests;

[TestClass]
public class ScrapbookTests : ResilientFunctions.Tests.TestTemplates.ScrapbookTests
{
    [TestMethod]
    public override Task SunshineScenario()
        => SunshineScenario(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override async Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected()
        => await ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ScrapbookIsUsedWhenSpecifiedAtRegistration()
        => ScrapbookIsUsedWhenSpecifiedAtRegistration(NoSql.AutoCreateAndInitializeStore());
}