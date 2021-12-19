using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests
{
    [TestClass]
    public class SunshineTests : ResilientFunctions.Tests.RFunctionTests.SunshineTests
    {
        private SqlServerFunctionStore Store { get; } = new SqlServerFunctionStore(Sql.ConnFunc);

        [TestInitialize]
        public async Task SetUp()
        {
            await Store.Initialize();
            await Store.Truncate();
        }

        [TestMethod]
        public override Task SunshineScenarioFunc() => SunshineScenarioFunc(Store);

        [TestMethod]
        public override Task SunshineScenarioFuncWithScrapbook() => SunshineScenarioFuncWithScrapbook(Store);

        [TestMethod]
        public override Task SunshineScenarioAction() => SunshineScenarioAction(Store);

        [TestMethod]
        public override Task SunshineScenarioActionWithScrapbook() => SunshineScenarioActionWithScrapbook(Store);
    }
}