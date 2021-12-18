using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class RFunctionTests : Cleipnir.ResilientFunctions.Tests.RFunctionTests
    {
        private SqlServerFunctionStore Store { get; } = new SqlServerFunctionStore(Sql.ConnFunc);

        [TestInitialize]
        public async Task SetUp()
        {
            await Store.Initialize();
            await Store.Truncate();
        }
        
        [TestMethod]
        public override Task SunshineScenario() => SunshineScenario(Store);

        [TestMethod]
        public override Task NonCompletedFunctionIsCompletedByWatchDog() 
            => NonCompletedFunctionIsCompletedByWatchDog(Store);

        [TestMethod]
        public override Task PostponedFunctionIsCompletedByWatchDog()
            => NonCompletedFunctionIsCompletedByWatchDog(Store);
    }
}