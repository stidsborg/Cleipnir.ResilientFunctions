using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class ScrapbookTests : Cleipnir.ResilientFunctions.Tests.ScrapbookTests
    {
        [TestMethod]
        public override async Task SunshineScenario()
            => await SunshineScenario(
                await Sql.CreateAndInitializeStore(nameof(ScrapbookTests), nameof(SunshineScenario))
            );

        public override async Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected()
            => await ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected(
                await Sql.CreateAndInitializeStore(
                    nameof(ScrapbookTests), 
                    nameof(ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected)
                )
            );
    }
}