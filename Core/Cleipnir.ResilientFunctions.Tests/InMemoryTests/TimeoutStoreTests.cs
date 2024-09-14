using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class TimeoutStoreTests : TestTemplates.TimeoutStoreTests
{
    [TestMethod]
    public override Task TimeoutCanBeCreatedFetchedAndRemoveSuccessfully()
        => TimeoutCanBeCreatedFetchedAndRemoveSuccessfully(FunctionStoreFactory.Create().SelectAsync(fs => fs.TimeoutStore));

    [TestMethod]
    public override Task ExistingTimeoutCanUpdatedSuccessfully()
        => ExistingTimeoutCanUpdatedSuccessfully(FunctionStoreFactory.Create().SelectAsync(fs => fs.TimeoutStore));

    [TestMethod]
    public override Task OverwriteFalseDoesNotAffectExistingTimeout()
        => OverwriteFalseDoesNotAffectExistingTimeout(FunctionStoreFactory.Create().SelectAsync(fs => fs.TimeoutStore));

    [TestMethod]
    public override Task RegisteredTimeoutIsReturnedFromRegisteredTimeouts()
        => RegisteredTimeoutIsReturnedFromRegisteredTimeouts(FunctionStoreFactory.Create().SelectAsync(fs => fs.TimeoutStore));

    [TestMethod]
    public override Task TimeoutStoreCanBeInitializedMultipleTimes()
        => TimeoutStoreCanBeInitializedMultipleTimes(FunctionStoreFactory.Create().SelectAsync(fs => fs.TimeoutStore));

    [TestMethod]
    public override Task RegisteredTimeoutIsReturnedFromRegisteredTimeoutsForFunctionId()
        => RegisteredTimeoutIsReturnedFromRegisteredTimeoutsForFunctionId(FunctionStoreFactory.Create().SelectAsync(fs => fs.TimeoutStore));

    [TestMethod]
    public override Task TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout()
        => TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout(FunctionStoreFactory.Create().SelectAsync(fs => fs.TimeoutStore));

    [TestMethod]
    public override Task TimeoutsForDifferentTypesCanBeCreatedFetchedSuccessfully()
        => TimeoutsForDifferentTypesCanBeCreatedFetchedSuccessfully(FunctionStoreFactory.Create().SelectAsync(fs => fs.TimeoutStore));

    [TestMethod]
    public override Task CancellingNonExistingTimeoutDoesNotResultInIO()
        => CancellingNonExistingTimeoutDoesNotResultInIO(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));
}