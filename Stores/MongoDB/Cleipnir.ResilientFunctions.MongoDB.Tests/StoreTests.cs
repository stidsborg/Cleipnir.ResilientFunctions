using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests;

[TestClass]
public class StoreTests : ResilientFunctions.Tests.TestTemplates.StoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest() 
        => SunshineScenarioTest(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SignOfLifeIsUpdatedWhenAsExpected() 
        => SignOfLifeIsUpdatedWhenAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenNotAsExpected() 
        => SignOfLifeIsNotUpdatedWhenNotAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task BecomeLeaderSucceedsWhenEpochIsAsExpected() 
        => BecomeLeaderSucceedsWhenEpochIsAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task BecomeLeaderFailsWhenEpochIsNotAsExpected()
        => BecomeLeaderFailsWhenEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task BecomeLeaderWithParamAndScrapbookSucceedsWhenEpochIsAsExpected()
        => BecomeLeaderWithParamAndScrapbookSucceedsWhenEpochIsAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task BecomeLeaderWithParamAndScrapbookFailsWhenEpochIsNotAsExpected()
        => BecomeLeaderWithParamAndScrapbookFailsWhenEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task CreatingTheSameFunctionTwiceReturnsFalse()
        => CreatingTheSameFunctionTwiceReturnsFalse(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut()
        => FunctionPostponedUntilAfterExpiresBeforeIsFilteredOut(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FunctionPostponedUntilBeforeExpiresIsNotFilteredOut()
        => FunctionPostponedUntilBeforeExpiresIsNotFilteredOut(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task InitializeCanBeInvokedMultipleTimesSuccessfully()
        => InitializeCanBeInvokedMultipleTimesSuccessfully(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency()
        => CreatedCrashedCheckFrequencyOfCreatedFunctionIsSameAsExecutingFunctionCrashCheckFrequency(
            NoSql.AutoCreateAndInitializeStore()
        );

    [TestMethod]
    public override Task LeaderElectionSpecifiedCrashCheckFrequencyIsSameAsExecutingFunctionCrashCheckFrequency()
        => LeaderElectionSpecifiedCrashCheckFrequencyIsSameAsExecutingFunctionCrashCheckFrequency(
            NoSql.AutoCreateAndInitializeStore()
        );

    [TestMethod]
    public override Task IncrementEpochSucceedsWhenEpochIsAsExpected()
        => IncrementEpochSucceedsWhenEpochIsAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task IncrementEpochFailsWhenEpochIsNotAsExpected()
        => IncrementEpochFailsWhenEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());
}