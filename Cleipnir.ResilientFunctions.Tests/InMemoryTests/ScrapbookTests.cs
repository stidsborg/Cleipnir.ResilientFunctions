using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests
{
    [TestClass]
    public class ScrapbookTests : TestTemplates.ScrapbookTests
    {
        [TestMethod]
        public override Task SunshineScenario()
            => SunshineScenario(new InMemoryFunctionStore());

        public override Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected()
            => ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected(new InMemoryFunctionStore());
    }
}