using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

[TestClass]
public class TimeoutStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.TimeoutStoreTests
{
    [TestMethod]
    public override Task TimeoutCanBeCreatedFetchedAndRemoveSuccessfully()
        => TimeoutCanBeCreatedFetchedAndRemoveSuccessfully(FunctionStoreFactory.FunctionStore.TimeoutStore.ToTask());

    [TestMethod]
    public override Task ExistingTimeoutCanUpdatedSuccessfully()
        => ExistingTimeoutCanUpdatedSuccessfully(FunctionStoreFactory.FunctionStore.TimeoutStore.ToTask());

    [TestMethod]
    public override Task OverwriteFalseDoesNotAffectExistingTimeout()
        => OverwriteFalseDoesNotAffectExistingTimeout(FunctionStoreFactory.FunctionStore.TimeoutStore.ToTask());

    [TestMethod]
    public override Task TimeoutStoreCanBeInitializedMultipleTimes()
        => TimeoutStoreCanBeInitializedMultipleTimes(FunctionStoreFactory.FunctionStore.TimeoutStore.ToTask());
}