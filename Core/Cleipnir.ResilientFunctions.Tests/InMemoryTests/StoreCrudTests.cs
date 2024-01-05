using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class StoreCrudTests : TestTemplates.StoreCrudTests
{
    [TestMethod]
    public override Task FunctionCanBeCreatedWithASingleParameterSuccessfully()
        => FunctionCanBeCreatedWithASingleParameterSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FetchingNonExistingFunctionReturnsNull()
        => FetchingNonExistingFunctionReturnsNull(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task LeaseIsUpdatedWhenCurrentEpochMatches()
        => LeaseIsUpdatedWhenCurrentEpochMatches(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task LeaseIsNotUpdatedWhenCurrentEpochIsDifferent()
        => LeaseIsNotUpdatedWhenCurrentEpochIsDifferent(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task UpdateScrapbookSunshineScenario()
        => UpdateScrapbookSunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected()
        => ScrapbookUpdateFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingFunctionCanBeDeleted()
        => ExistingFunctionCanBeDeleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonExistingFunctionCanBeDeleted()
        => NonExistingFunctionCanBeDeleted(FunctionStoreFactory.Create());    

    [TestMethod]
    public override Task ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected()
        => ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParameterAndScrapbookCanBeUpdatedOnExistingFunction()
        => ParameterAndScrapbookCanBeUpdatedOnExistingFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParameterCanBeUpdatedOnExistingFunction()
        => ParameterCanBeUpdatedOnExistingFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScrapbookCanBeUpdatedOnExistingFunction()
        => ScrapbookCanBeUpdatedOnExistingFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParameterAndScrapbookAreNotUpdatedWhenEpochDoesNotMatch()
        => ParameterAndScrapbookAreNotUpdatedWhenEpochDoesNotMatch(FunctionStoreFactory.Create());
}