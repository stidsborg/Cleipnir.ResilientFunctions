using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MySQL.Tests;

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
    public override Task OverwriteFalseDoesNotAffectExistingTimeout()
        => OverwriteFalseDoesNotAffectExistingTimeout(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));

    [TestMethod]
    public override Task RegisteredTimeoutIsReturnedFromTimeoutProvider()
        => RegisteredTimeoutIsReturnedFromTimeoutProvider(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));

    [TestMethod]
    public override Task TimeoutStoreCanBeInitializedMultipleTimes()
        => TimeoutStoreCanBeInitializedMultipleTimes(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));
    
    [TestMethod]
    public override Task RegisteredTimeoutIsReturnedFromTimeoutProviderForFunctionId()
        => RegisteredTimeoutIsReturnedFromTimeoutProviderForFunctionId(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));
    
    [TestMethod]
    public override Task TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout()
        => TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));
}