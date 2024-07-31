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
        => TimeoutCanBeCreatedFetchedAndRemoveSuccessfully(new InMemoryTimeoutStore().CastTo<ITimeoutStore>().ToTask());

    [TestMethod]
    public override Task ExistingTimeoutCanUpdatedSuccessfully()
        => ExistingTimeoutCanUpdatedSuccessfully(new InMemoryTimeoutStore().CastTo<ITimeoutStore>().ToTask());

    [TestMethod]
    public override Task OverwriteFalseDoesNotAffectExistingTimeout()
        => OverwriteFalseDoesNotAffectExistingTimeout(new InMemoryTimeoutStore().CastTo<ITimeoutStore>().ToTask());

    [TestMethod]
    public override Task RegisteredTimeoutIsReturnedFromTimeoutProvider()
        => RegisteredTimeoutIsReturnedFromTimeoutProvider(new InMemoryTimeoutStore().CastTo<ITimeoutStore>().ToTask());

    [TestMethod]
    public override Task TimeoutStoreCanBeInitializedMultipleTimes()
        => TimeoutStoreCanBeInitializedMultipleTimes(new InMemoryTimeoutStore().CastTo<ITimeoutStore>().ToTask());

    [TestMethod]
    public override Task RegisteredTimeoutIsReturnedFromTimeoutProviderForFunctionId()
        => RegisteredTimeoutIsReturnedFromTimeoutProviderForFunctionId(new InMemoryTimeoutStore().CastTo<ITimeoutStore>().ToTask());

    [TestMethod]
    public override Task TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout()
        => TimeoutIsNotRegisteredAgainWhenProviderAlreadyContainsTimeout(new InMemoryTimeoutStore().CastTo<ITimeoutStore>().ToTask());

    [TestMethod]
    public override Task CancellingNonExistingTimeoutDoesNotResultInIO()
        => CancellingNonExistingTimeoutDoesNotResultInIO(FunctionStoreFactory.Create().SelectAsync(s => s.TimeoutStore));
}