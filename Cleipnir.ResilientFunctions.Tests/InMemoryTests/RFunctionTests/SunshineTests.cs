using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests
{
    [TestClass]
    public class SunshineTests : TestTemplates.RFunctionTests.SunshineTests
    {
        [TestMethod]
        public override Task SunshineScenarioFunc()
            => SunshineScenarioFunc(new InMemoryFunctionStore());
        
        [TestMethod]
        public override Task SunshineScenarioFuncWithScrapbook()
            => SunshineScenarioFuncWithScrapbook(new InMemoryFunctionStore());

        [TestMethod]
        public override Task SunshineScenarioAction()
            => SunshineScenarioAction(new InMemoryFunctionStore());

        [TestMethod]
        public override Task SunshineScenarioActionWithScrapbook()
            => SunshineScenarioActionWithScrapbook(new InMemoryFunctionStore());

        [TestMethod]
        public override Task SunshineScenarioNullReturningFunc()
            => SunshineScenarioNullReturningFunc(new InMemoryFunctionStore());

        [TestMethod]
        public override Task SunshineScenarioNullReturningFuncWithScrapbook()
            => SunshineScenarioNullReturningFuncWithScrapbook(new InMemoryFunctionStore());
    }
}