using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Utils.Register;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.UtilsTests;

[TestClass]
public class RegisterTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests.RegisterTests
{
    [TestMethod]
    public override Task SetValueWithNoExistingValueSucceeds()
        => SetValueWithNoExistingValueSucceeds(CreateInMemoryRegister());

    [TestMethod]
    public override Task CompareAndSwapWithNoExistingValueSucceeds()
        => CompareAndSwapWithNoExistingValueSucceeds(CreateInMemoryRegister());

    [TestMethod]
    public override Task CompareAndSwapFailsWithNoExistingValue()
        => CompareAndSwapFailsWithNoExistingValue(CreateInMemoryRegister());

    [TestMethod]
    public override Task SetValueIfEmptyFailsWhenRegisterHasExistingValue()
        => SetValueIfEmptyFailsWhenRegisterHasExistingValue(CreateInMemoryRegister());

    [TestMethod]
    public override Task CompareAndSwapSucceedsIfAsExpected()
        => CompareAndSwapSucceedsIfAsExpected(CreateInMemoryRegister());

    [TestMethod]
    public override Task CompareAndSwapSucceedsIfAsExpectedIgnoreIfNoExisting()
        => CompareAndSwapSucceedsIfAsExpectedIgnoreIfNoExisting(CreateInMemoryRegister());

    [TestMethod]
    public override Task ExistsIfFalseForNonExistingRegister()
        => ExistsIfFalseForNonExistingRegister(CreateInMemoryRegister());

    [TestMethod]
    public override Task ExistingValueIsNullForNonExistingRegister()
        => ExistingValueIsNullForNonExistingRegister(CreateInMemoryRegister());

    [TestMethod]
    public override Task DeleteSucceedsForNonExistingRegister()
        => DeleteSucceedsForNonExistingRegister(CreateInMemoryRegister());

    [TestMethod]
    public override Task DeleteSucceedsForExistingRegister()
        => DeleteSucceedsForExistingRegister(CreateInMemoryRegister());

    [TestMethod]
    public override Task DeleteSucceedsWithExpectedValueForExistingRegister()
        => DeleteSucceedsWithExpectedValueForExistingRegister(CreateInMemoryRegister());

    [TestMethod]
    public override Task DeleteFailsWhenNonExpectedValueForExistingRegister()
        => DeleteFailsWhenNonExpectedValueForExistingRegister(CreateInMemoryRegister());

    private Task<IRegister> CreateInMemoryRegister() => new InMemoryRegister().CastTo<IRegister>().ToTask();
}