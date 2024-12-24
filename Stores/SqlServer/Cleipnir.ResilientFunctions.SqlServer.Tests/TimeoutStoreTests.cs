using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class TimeoutStoreTests : ResilientFunctions.Tests.TestTemplates.TimeoutStoreTests
{
    [TestMethod]
    public override Task TimeoutCanBeCreatedFetchedAndRemoveSuccessfully()
        => TimeoutCanBeCreatedFetchedAndRemoveSuccessfully(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));

    [TestMethod]
    public override Task ExistingTimeoutCanUpdatedSuccessfully()
        => ExistingTimeoutCanUpdatedSuccessfully(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));

    [TestMethod]
    public override Task TimeoutStoreCanBeInitializedMultipleTimes()
        => TimeoutStoreCanBeInitializedMultipleTimes(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));
    
    [TestMethod]
    public override Task RegisteredTimeoutIsReturnedFromRegisteredTimeouts()
        => RegisteredTimeoutIsReturnedFromRegisteredTimeouts(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task OverwriteFalseDoesNotAffectExistingTimeout()
        => OverwriteFalseDoesNotAffectExistingTimeout(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));
    
    [TestMethod]
    public override Task RegisteredTimeoutIsReturnedFromRegisteredTimeoutsForFunctionId()
        => RegisteredTimeoutIsReturnedFromRegisteredTimeoutsForFunctionId(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout()
        => TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task TimeoutsForDifferentTypesCanBeCreatedFetchedSuccessfully()
        => TimeoutsForDifferentTypesCanBeCreatedFetchedSuccessfully(FunctionStoreFactory.Create().SelectAsync(fs => fs.TimeoutStore));

    [TestMethod]
    public override Task CancellingNonExistingTimeoutDoesResultInIO()
        => CancellingNonExistingTimeoutDoesResultInIO(FunctionStoreFactory.Create());
}