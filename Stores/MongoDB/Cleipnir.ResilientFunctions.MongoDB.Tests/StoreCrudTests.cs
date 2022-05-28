using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests;

[TestClass]
public class StoreCrudTests : ResilientFunctions.Tests.TestTemplates.StoreCrudTests
{
    [TestMethod]
    public override Task FunctionCanBeCreatedWithASingleParameterSuccessfully()
        => FunctionCanBeCreatedWithASingleParameterSuccessfully(NoSql.AutoCreateAndInitializeStore());
        
    [TestMethod]
    public override Task FunctionCanBeCreatedWithATwoParametersSuccessfully()
        => FunctionCanBeCreatedWithATwoParametersSuccessfully(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully()
        => FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FetchingNonExistingFunctionReturnsNull()
        => FetchingNonExistingFunctionReturnsNull(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected()
        => SignOfLifeIsNotUpdatedWhenItIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UpdateScrapbookSunshineScenario()
        => UpdateScrapbookSunshineScenario(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected()
        => ScrapbookUpdateFailsWhenEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());
}