using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class StoreTests : Tests.StoreTests
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
    }
}