using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class RFunctionTests : Tests.RFunctionTests
    {
        [TestMethod]
        public override Task SunshineScenario() 
            => SunshineScenario(new InMemoryFunctionStore());

        [TestMethod]
        public override Task NonCompletedFunctionIsCompletedByWatchDog() 
            => NonCompletedFunctionIsCompletedByWatchDog(new InMemoryFunctionStore());

        [TestMethod]
        public override Task PostponedFunctionIsCompletedByWatchDog()
            => PostponedFunctionIsCompletedByWatchDog(new InMemoryFunctionStore());
    }
}