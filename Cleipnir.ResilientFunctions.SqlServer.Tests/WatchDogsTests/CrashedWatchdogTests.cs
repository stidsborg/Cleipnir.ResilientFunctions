using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.WatchDogsTests
{
    [TestClass]
    public class CrashedWatchdogTests : ResilientFunctions.Tests.TestTemplates.WatchDogsTests.CrashedWatchdogTests
    {
        [TestMethod]
        public override async Task CrashedFunctionInvocationIsCompletedByWatchDog()
            => await CrashedFunctionInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(CrashedWatchdogTests),
                    nameof(CrashedFunctionInvocationIsCompletedByWatchDog)
                )
            );

        [TestMethod]
        public override async Task CrashedFunctionWithScrapbookInvocationIsCompletedByWatchDog()
            => await CrashedFunctionWithScrapbookInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(CrashedWatchdogTests),
                    nameof(CrashedFunctionWithScrapbookInvocationIsCompletedByWatchDog)
                )
            );

        [TestMethod]
        public override async Task CrashedActionInvocationIsCompletedByWatchDog()
            => await CrashedFunctionInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(CrashedWatchdogTests),
                    nameof(CrashedActionInvocationIsCompletedByWatchDog)
                )
            );

        [TestMethod]
        public override async Task CrashedActionWithScrapbookInvocationIsCompletedByWatchDog()
            => await CrashedActionWithScrapbookInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(CrashedWatchdogTests),
                    nameof(CrashedActionWithScrapbookInvocationIsCompletedByWatchDog)
                )
            );
    }
}