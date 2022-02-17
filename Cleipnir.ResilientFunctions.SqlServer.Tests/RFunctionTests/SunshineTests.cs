using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests
{
    [TestClass]
    public class SunshineTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SunshineTests
    {
        [TestMethod]
        public override Task SunshineScenarioFunc() 
            => SunshineScenarioFunc(Sql.AutoCreateAndInitializeStore().Result);

        [TestMethod]
        public override Task SunshineScenarioFuncWithScrapbook() 
            => SunshineScenarioFuncWithScrapbook(Sql.AutoCreateAndInitializeStore().Result);

        [TestMethod]
        public override Task SunshineScenarioAction() 
            => SunshineScenarioAction(Sql.AutoCreateAndInitializeStore().Result);

        [TestMethod]
        public override Task SunshineScenarioActionWithScrapbook() 
            => SunshineScenarioActionWithScrapbook(Sql.AutoCreateAndInitializeStore().Result);
    }
}