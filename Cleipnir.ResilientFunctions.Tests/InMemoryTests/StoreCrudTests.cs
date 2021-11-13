using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class StoreCrudTests : Cleipnir.ResilientFunctions.Tests.StoreCrudTests
    {
        [TestMethod]
        public override Task SingleParameterSunshineScenario()
            => SingleParameterSunshineScenario(new InMemoryFunctionStore());

        [TestMethod]
        public override Task DoubleParameterSunshineScenario()
            => DoubleParameterSunshineScenario(new InMemoryFunctionStore());

        [TestMethod]
        public override Task DoubleParameterWithScrapbookSunshineScenario()
            => DoubleParameterWithScrapbookSunshineScenario(new InMemoryFunctionStore());

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
        public override Task UpdateScrapbookFailsWhenTimestampIsNotAsExpected()
            => UpdateScrapbookFailsWhenTimestampIsNotAsExpected(new InMemoryFunctionStore());

        [TestMethod]
        public override Task OnlyNonCompletedFunctionsAreReturnedWhenStoreMethodIsInvoked()
            => OnlyNonCompletedFunctionsAreReturnedWhenStoreMethodIsInvoked(new InMemoryFunctionStore());
    }
}