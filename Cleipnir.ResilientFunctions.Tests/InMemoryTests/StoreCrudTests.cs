using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class StoreCrudTests : TestTemplates.StoreCrudTests
{
    [TestMethod]
    public override Task FunctionCanBeCreatedWithASingleParameterSuccessfully()
        => FunctionCanBeCreatedWithASingleParameterSuccessfully(new InMemoryFunctionStore());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithATwoParametersSuccessfully()
        => FunctionCanBeCreatedWithATwoParametersSuccessfully(new InMemoryFunctionStore());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully()
        => FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully(new InMemoryFunctionStore());

    [TestMethod]
    public override Task FetchingNonExistingFunctionReturnsNull()
        => FetchingNonExistingFunctionReturnsNull(new InMemoryFunctionStore());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected()
        => SignOfLifeIsNotUpdatedWhenItIsNotAsExpected(new InMemoryFunctionStore());

    [TestMethod]
    public override Task UpdateScrapbookSunshineScenario()
        => UpdateScrapbookSunshineScenario(new InMemoryFunctionStore());

    [TestMethod]
    public override Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected()
        => ScrapbookUpdateFailsWhenEpochIsNotAsExpected(new InMemoryFunctionStore());

    [TestMethod]
    public override Task GetFunctionsWithStatusOnlyReturnsSucceededFunction()
        => GetFunctionsWithStatusOnlyReturnsSucceededFunction(new InMemoryFunctionStore());
}