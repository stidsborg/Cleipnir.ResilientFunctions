using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class StoreTests : ResilientFunctions.Tests.TestTemplates.StoreTests
    {
        private SqlServerFunctionStore Store { get; set; } = new SqlServerFunctionStore(Sql.ConnFunc); //suppress compiler warning

        [TestInitialize]
        public async Task SetUp()
        {
            Store = new SqlServerFunctionStore(Sql.ConnFunc);
            await Store.Initialize();
            await Store.Truncate();
        }

        [TestMethod]
        public override Task SunshineScenarioTest() => SunshineScenarioTest(Store);

        [TestMethod]
        public override Task SignOfLifeIsUpdatedWhenAsExpected() => SignOfLifeIsUpdatedWhenAsExpected(Store);

        [TestMethod]
        public override Task SignOfLifeIsNotUpdatedWhenNotAsExpected() => SignOfLifeIsNotUpdatedWhenNotAsExpected(Store);

        [TestMethod]
        public override Task BecomeLeaderSucceedsWhenEpochIsAsExpected() 
            => BecomeLeaderSucceedsWhenEpochIsAsExpected(Store);

        [TestMethod]
        public override Task BecomeLeaderFailsWhenEpochIsNotAsExpected()
            => BecomeLeaderFailsWhenEpochIsNotAsExpected(Store);
    }
}