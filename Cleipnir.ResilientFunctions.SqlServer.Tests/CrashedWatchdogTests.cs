using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class CrashedWatchdogTests : Cleipnir.ResilientFunctions.Tests.CrashedWatchdogTests
    {
        [TestMethod]
        public override async Task CrashedFunctionInvocationIsCompletedByWatchDog()
            => await CrashedFunctionInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(CrashedFunctionInvocationIsCompletedByWatchDog)
                )
            );
    }
}