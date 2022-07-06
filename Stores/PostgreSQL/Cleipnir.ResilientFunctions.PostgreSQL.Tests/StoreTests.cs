using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests;

[TestClass]
public class StoreTests : ResilientFunctions.Tests.TestTemplates.StoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest() 
        => SunshineScenarioTest(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SignOfLifeIsUpdatedWhenAsExpected() 
        => SignOfLifeIsUpdatedWhenAsExpected(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenNotAsExpected() 
        => SignOfLifeIsNotUpdatedWhenNotAsExpected(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task BecomeLeaderSucceedsWhenEpochIsAsExpected() 
        => BecomeLeaderSucceedsWhenEpochIsAsExpected(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task BecomeLeaderFailsWhenEpochIsNotAsExpected()
        => BecomeLeaderFailsWhenEpochIsNotAsExpected(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task CreatingTheSameFunctionTwiceReturnsFalse()
        => CreatingTheSameFunctionTwiceReturnsFalse(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut()
        => FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut()
        => FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task InitializeCanBeInvokedMultipleTimesSuccessfully()
        => InitializeCanBeInvokedMultipleTimesSuccessfully(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency()
        => CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency(
            Sql.AutoCreateAndInitializeStore()
        );

    [TestMethod]
    public override Task LeaderElectionSpecifiedCrashCheckFrequencyIsSameAsExecutingFunctionCrashCheckFrequency()
        => LeaderElectionSpecifiedCrashCheckFrequencyIsSameAsExecutingFunctionCrashCheckFrequency(
            Sql.AutoCreateAndInitializeStore()
        );
}