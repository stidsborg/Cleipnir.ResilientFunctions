using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class ScrapbookTests : ResilientFunctions.Tests.TestTemplates.ScrapbookTests
    {
        [TestMethod]
        public override Task SunshineScenario()
            => SunshineScenario(Sql.AutoCreateAndInitializeStore().Result);

        [TestMethod]
        public override async Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected()
            => await ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected(Sql.AutoCreateAndInitializeStore().Result);
    }
}