using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class RFunctionScrapbookTests : Tests.RFunctionScrapbookTests
    {
        [TestMethod]
        public override Task SunshineScenario() 
            => SunshineScenario(new InMemoryFunctionStore());
    }
}