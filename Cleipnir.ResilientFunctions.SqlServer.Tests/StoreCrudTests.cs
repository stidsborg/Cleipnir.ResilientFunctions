using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class StoreCrudTests : ResilientFunctions.Tests.TestTemplates.StoreCrudTests
{
    [TestMethod]
    public override Task FunctionCanBeCreatedWithASingleParameterSuccessfully()
        => FunctionCanBeCreatedWithASingleParameterSuccessfully(Sql.AutoCreateAndInitializeStore().Result);
        
    [TestMethod]
    public override Task FunctionCanBeCreatedWithATwoParametersSuccessfully()
        => FunctionCanBeCreatedWithATwoParametersSuccessfully(Sql.AutoCreateAndInitializeStore().Result);

    [TestMethod]
    public override Task FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully()
        => FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully(Sql.AutoCreateAndInitializeStore().Result);

    [TestMethod]
    public override Task FetchingNonExistingFunctionReturnsNull()
        => FetchingNonExistingFunctionReturnsNull(Sql.AutoCreateAndInitializeStore().Result);

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected()
        => SignOfLifeIsNotUpdatedWhenItIsNotAsExpected(Sql.AutoCreateAndInitializeStore().Result);

    [TestMethod]
    public override Task UpdateScrapbookSunshineScenario()
        => UpdateScrapbookSunshineScenario(Sql.AutoCreateAndInitializeStore().Result);

    [TestMethod]
    public override Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected()
        => ScrapbookUpdateFailsWhenEpochIsNotAsExpected(Sql.AutoCreateAndInitializeStore().Result);

    [TestMethod]
    public override Task GetFunctionsWithStatusOnlyReturnsSucceededFunction()
        => GetFunctionsWithStatusOnlyReturnsSucceededFunction(Sql.AutoCreateAndInitializeStore().Result);
}