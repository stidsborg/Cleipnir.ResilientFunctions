using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class RFunctionTests : ResilientFunctions.Tests.RFunctionTests
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
        public override Task SunshineScenario() => SunshineScenario(Store);

        [TestMethod]
        public override Task NonCompletedFunctionIsCompletedByWatchDog() 
            => NonCompletedFunctionIsCompletedByWatchDog(Store);

    }
}