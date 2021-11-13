using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class StoreCrudTests : Cleipnir.ResilientFunctions.Tests.StoreCrudTests
    {
        [TestMethod]
        public override async Task SingleParameterSunshineScenario()
            => await SingleParameterSunshineScenario(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(SingleParameterSunshineScenario)
                )
            );

        [TestMethod]
        public override async Task DoubleParameterSunshineScenario()
            => await DoubleParameterSunshineScenario(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(DoubleParameterSunshineScenario)
                )
            );

        [TestMethod]
        public override async Task DoubleParameterWithScrapbookSunshineScenario()
            => await DoubleParameterWithScrapbookSunshineScenario(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(DoubleParameterWithScrapbookSunshineScenario)
                )
            );

        [TestMethod]
        public override async Task FetchingNonExistingFunctionReturnsNull()
            => await FetchingNonExistingFunctionReturnsNull(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(FetchingNonExistingFunctionReturnsNull)
                )
            );

        [TestMethod]
        public override async Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected()
            => await SignOfLifeIsNotUpdatedWhenItIsNotAsExpected(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(SignOfLifeIsNotUpdatedWhenItIsNotAsExpected)
                )
            );

        [TestMethod]
        public override async Task UpdateScrapbookSunshineScenario()
            => await UpdateScrapbookSunshineScenario(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(UpdateScrapbookSunshineScenario)
                )
            );

        [TestMethod]
        public override async Task UpdateScrapbookFailsWhenTimestampIsNotAsExpected()
            => await UpdateScrapbookFailsWhenTimestampIsNotAsExpected(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(UpdateScrapbookFailsWhenTimestampIsNotAsExpected)
                )
            );

        [TestMethod]
        public override async Task OnlyNonCompletedFunctionsAreReturnedWhenStoreMethodIsInvoked()
            => await OnlyNonCompletedFunctionsAreReturnedWhenStoreMethodIsInvoked(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(OnlyNonCompletedFunctionsAreReturnedWhenStoreMethodIsInvoked)
                )
            );
    }
}