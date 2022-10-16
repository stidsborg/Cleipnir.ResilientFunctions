using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests;

[TestClass]
public class StoreCrudTests : ResilientFunctions.Tests.TestTemplates.StoreCrudTests
{
    [TestMethod]
    public override Task FunctionCanBeCreatedWithASingleParameterSuccessfully()
        => FunctionCanBeCreatedWithASingleParameterSuccessfully(NoSql.AutoCreateAndInitializeStore());
        
    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersSuccessfully(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FetchingNonExistingFunctionReturnsNull()
        => FetchingNonExistingFunctionReturnsNull(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SignOfLifeIsUpdatedWhenCurrentEpochMatches()
        => SignOfLifeIsUpdatedWhenCurrentEpochMatches(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent()
        => SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task UpdateScrapbookSunshineScenario()
        => UpdateScrapbookSunshineScenario(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected()
        => ScrapbookUpdateFailsWhenEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExistingFunctionCanBeDeleted()
        => ExistingFunctionCanBeDeleted(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task NonExistingFunctionCanBeDeleted()
        => NonExistingFunctionCanBeDeleted(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected()
        => ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExistingFunctionIsNotDeletedWhenStatusIsNotAsExpected()
        => ExistingFunctionIsNotDeletedWhenStatusIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ExistingFunctionIsNotDeletedWhenStatusAndEpochIsNotAsExpected()
        => ExistingFunctionIsNotDeletedWhenStatusAndEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ParameterAndScrapbookCanBeUpdatedOnExistingFunction()
        => ParameterAndScrapbookCanBeUpdatedOnExistingFunction(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ParameterCanBeUpdatedOnExistingFunction()
        => ParameterCanBeUpdatedOnExistingFunction(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScrapbookCanBeUpdatedOnExistingFunction()
        => ScrapbookCanBeUpdatedOnExistingFunction(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ParameterAndScrapbookAreNotUpdatedWhenEpochDoesNotMatch()
        => ParameterAndScrapbookAreNotUpdatedWhenEpochDoesNotMatch(NoSql.AutoCreateAndInitializeStore());
}