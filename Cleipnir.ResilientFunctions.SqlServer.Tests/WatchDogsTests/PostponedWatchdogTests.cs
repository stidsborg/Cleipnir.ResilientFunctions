using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.WatchDogsTests
{
    [TestClass]
    public class PostponedWatchdogTests : ResilientFunctions.Tests.TestTemplates.WatchDogsTests.PostponedWatchdogTests
    {
        [TestMethod]
        public override async Task PostponedFunctionInvocationIsCompletedByWatchDog()
            => await PostponedFunctionInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(PostponedFunctionInvocationIsCompletedByWatchDog)
                )
            );

        [TestMethod]
        public override async Task PostponedFunctionWithScrapbookInvocationIsCompletedByWatchDog()
            => await PostponedFunctionWithScrapbookInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(PostponedFunctionWithScrapbookInvocationIsCompletedByWatchDog)
                )
            );

        [TestMethod]
        public override async Task PostponedActionInvocationIsCompletedByWatchDog()
            => await PostponedActionInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(PostponedActionInvocationIsCompletedByWatchDog)
                )
            );

        [TestMethod]
        public override async Task PostponedActionWithScrapbookInvocationIsCompletedByWatchDog()
            => await PostponedActionWithScrapbookInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(PostponedActionWithScrapbookInvocationIsCompletedByWatchDog)
                )
            );
    }
}