using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class StoreTests : TestTemplates.StoreTests
    {
        [TestMethod]
        public override Task SunshineScenarioTest() 
            => SunshineScenarioTest(new InMemoryFunctionStore());

        [TestMethod]
        public override Task SignOfLifeIsUpdatedWhenAsExpected() 
            => SignOfLifeIsUpdatedWhenAsExpected(new InMemoryFunctionStore());

        [TestMethod]
        public override Task SignOfLifeIsNotUpdatedWhenNotAsExpected()
            => SignOfLifeIsNotUpdatedWhenNotAsExpected(new InMemoryFunctionStore());

        [TestMethod]
        public override Task BecomeLeaderSucceedsWhenEpochIsAsExpected()
            => BecomeLeaderSucceedsWhenEpochIsAsExpected(new InMemoryFunctionStore());

        [TestMethod]
        public override Task BecomeLeaderFailsWhenEpochIsNotAsExpected()
            => BecomeLeaderFailsWhenEpochIsNotAsExpected(new InMemoryFunctionStore());

        [TestMethod]
        public override Task NonExistingFunctionCanBeBarricaded()
            => NonExistingFunctionCanBeBarricaded(new InMemoryFunctionStore());

        [TestMethod]
        public override Task ExistingFunctionCannotBeBarricaded()
            => ExistingFunctionCannotBeBarricaded(new InMemoryFunctionStore());
    }
}