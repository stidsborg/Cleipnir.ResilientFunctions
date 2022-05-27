using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests;

[TestClass]
public class ScrapbookTests : ResilientFunctions.Tests.TestTemplates.ScrapbookTests
{
    [TestMethod]
    public override Task SunshineScenario()
        => SunshineScenario(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override async Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected()
        => await ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected(Sql.AutoCreateAndInitializeStore());
}