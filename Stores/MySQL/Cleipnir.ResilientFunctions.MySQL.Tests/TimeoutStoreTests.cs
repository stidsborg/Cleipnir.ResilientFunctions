using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests;

[TestClass]
public class TimeoutStoreTests : ResilientFunctions.Tests.TestTemplates.TimeoutStoreTests
{
    [TestMethod]
    public override Task TimeoutCanBeCreatedFetchedAndRemoveSuccessfully()
        => TimeoutCanBeCreatedFetchedAndRemoveSuccessfully(Sql.AutoCreateAndInitializeStore().SelectAsync(s => s.TimeoutStore));

    [TestMethod]
    public override Task ExistingTimeoutCanUpdatedSuccessfully()
        => ExistingTimeoutCanUpdatedSuccessfully(Sql.AutoCreateAndInitializeStore().SelectAsync(s => s.TimeoutStore));

    [TestMethod]
    public override Task OverwriteFalseDoesNotAffectExistingTimeout()
        => OverwriteFalseDoesNotAffectExistingTimeout(Sql.AutoCreateAndInitializeStore().SelectAsync(s => s.TimeoutStore));

    [TestMethod]
    public override Task RegisteredTimeoutIsReturnedFromTimeoutProvider()
        => RegisteredTimeoutIsReturnedFromTimeoutProvider(Sql.AutoCreateAndInitializeStore().SelectAsync(s => s.TimeoutStore));

    [TestMethod]
    public override Task TimeoutStoreCanBeInitializedMultipleTimes()
        => TimeoutStoreCanBeInitializedMultipleTimes(Sql.AutoCreateAndInitializeStore().SelectAsync(s => s.TimeoutStore));
}