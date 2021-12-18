using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    [TestClass]
    public class PostponedWatchdogTests : Cleipnir.ResilientFunctions.Tests.PostponedWatchdogTests
    {
        [TestMethod]
        public override async Task PostponedFunctionInvocationIsCompletedByWatchDog()
            => await PostponedFunctionInvocationIsCompletedByWatchDog(
                await Sql.CreateAndInitializeStore(
                    nameof(StoreCrudTests),
                    nameof(PostponedFunctionInvocationIsCompletedByWatchDog)
                )
            );
    }
}