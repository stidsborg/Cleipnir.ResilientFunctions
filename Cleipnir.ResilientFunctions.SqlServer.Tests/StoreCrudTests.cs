using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class StoreCrudTests : Cleipnir.ResilientFunctions.Tests.StoreCrudTests
    {
        public override async Task FunctionCanBeCreatedWithASingleParameterSuccessfully()
            => await Sql.CreateAndInitializeStore(
                nameof(StoreCrudTests),
                nameof(FunctionCanBeCreatedWithASingleParameterSuccessfully)
            );

        public override async Task FunctionCanBeCreatedWithATwoParametersSuccessfully()
            => await Sql.CreateAndInitializeStore(
                nameof(StoreCrudTests),
                nameof(FunctionCanBeCreatedWithATwoParametersSuccessfully)
            );

        public override async Task FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully()
            => await Sql.CreateAndInitializeStore(
                nameof(StoreCrudTests),
                nameof(FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully)
            );

        public override async Task FetchingNonExistingFunctionReturnsNull()
            => await Sql.CreateAndInitializeStore(
                nameof(StoreCrudTests),
                nameof(FetchingNonExistingFunctionReturnsNull)
            );

        public override async Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected()
            => await Sql.CreateAndInitializeStore(
                nameof(StoreCrudTests),
                nameof(SignOfLifeIsNotUpdatedWhenItIsNotAsExpected)
            );

        public override async Task UpdateScrapbookSunshineScenario()
            => await Sql.CreateAndInitializeStore(
                nameof(StoreCrudTests),
                nameof(UpdateScrapbookSunshineScenario)
            );

        public override async Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected()
            => await Sql.CreateAndInitializeStore(
                nameof(StoreCrudTests),
                nameof(ScrapbookUpdateFailsWhenEpochIsNotAsExpected)
            );

        public override async Task GetFunctionsWithStatusOnlyReturnsSucceededFunction()
            => await Sql.CreateAndInitializeStore(
                nameof(StoreCrudTests),
                nameof(GetFunctionsWithStatusOnlyReturnsSucceededFunction)
            );
    }
}