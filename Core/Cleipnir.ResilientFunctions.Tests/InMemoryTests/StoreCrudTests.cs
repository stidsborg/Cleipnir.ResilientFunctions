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
        => FunctionCanBeCreatedWithASingleParameterSuccessfully(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersSuccessfully(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task FetchingNonExistingFunctionReturnsNull()
        => FetchingNonExistingFunctionReturnsNull(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SignOfLifeIsUpdatedWhenCurrentEpochMatches()
        => SignOfLifeIsUpdatedWhenCurrentEpochMatches(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent()
        => SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task UpdateScrapbookSunshineScenario()
        => UpdateScrapbookSunshineScenario(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected()
        => ScrapbookUpdateFailsWhenEpochIsNotAsExpected(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ExistingFunctionCanBeDeleted()
        => ExistingFunctionCanBeDeleted(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task NonExistingFunctionCanBeDeleted()
        => NonExistingFunctionCanBeDeleted(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());    

    [TestMethod]
    public override Task ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected()
        => ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ExistingFunctionIsNotDeletedWhenStatusIsNotAsExpected()
        => ExistingFunctionIsNotDeletedWhenStatusIsNotAsExpected(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ExistingFunctionIsNotDeletedWhenStatusAndEpochIsNotAsExpected()
        => ExistingFunctionIsNotDeletedWhenStatusAndEpochIsNotAsExpected(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ParameterAndScrapbookCanBeUpdatedOnExistingFunction()
        => ParameterAndScrapbookCanBeUpdatedOnExistingFunction(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ParameterCanBeUpdatedOnExistingFunction()
        => ParameterCanBeUpdatedOnExistingFunction(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ScrapbookCanBeUpdatedOnExistingFunction()
        => ScrapbookCanBeUpdatedOnExistingFunction(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task ParameterAndScrapbookAreNotUpdatedWhenEpochDoesNotMatch()
        => ParameterAndScrapbookAreNotUpdatedWhenEpochDoesNotMatch(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
}