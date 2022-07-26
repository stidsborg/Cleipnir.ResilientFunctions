using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests;

[TestClass]
public class StoreCrudTests : ResilientFunctions.Tests.TestTemplates.StoreCrudTests
{
    [TestMethod]
    public override Task FunctionCanBeCreatedWithASingleParameterSuccessfully()
        => FunctionCanBeCreatedWithASingleParameterSuccessfully(Sql.AutoCreateAndInitializeStore());
        
    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersSuccessfully(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FetchingNonExistingFunctionReturnsNull()
        => FetchingNonExistingFunctionReturnsNull(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SignOfLifeIsUpdatedWhenCurrentEpochMatches()
        => SignOfLifeIsUpdatedWhenCurrentEpochMatches(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent()
        => SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UpdateScrapbookSunshineScenario()
        => UpdateScrapbookSunshineScenario(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected()
        => ScrapbookUpdateFailsWhenEpochIsNotAsExpected(Sql.AutoCreateAndInitializeStore());
}