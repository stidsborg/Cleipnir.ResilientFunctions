﻿using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.SqlServer.Utils;
using Cleipnir.ResilientFunctions.Utils.Register;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.UtilTests;

[TestClass]
public class RegistrerTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests.RegisterTests
{
    [TestMethod]
    public override Task SetValueWithNoExistingValueSucceeds()
        => SetValueWithNoExistingValueSucceeds(CreateAndInitializeRegister());

    [TestMethod]
    public override Task CompareAndSwapWithNoExistingValueSucceeds()
        => CompareAndSwapWithNoExistingValueSucceeds(CreateAndInitializeRegister());

    [TestMethod]
    public override Task CompareAndSwapFailsWithNoExistingValue()
        => CompareAndSwapFailsWithNoExistingValue(CreateAndInitializeRegister());

    [TestMethod]
    public override Task SetValueIfEmptyFailsWhenRegisterHasExistingValue()
        => SetValueIfEmptyFailsWhenRegisterHasExistingValue(CreateAndInitializeRegister());

    [TestMethod]
    public override Task CompareAndSwapSucceedsIfAsExpected()
        => CompareAndSwapSucceedsIfAsExpected(CreateAndInitializeRegister());

    [TestMethod]
    public override Task CompareAndSwapSucceedsIfAsExpectedIgnoreIfNoExisting()
        => CompareAndSwapSucceedsIfAsExpectedIgnoreIfNoExisting(CreateAndInitializeRegister());

    [TestMethod]
    public override Task ExistsIfFalseForNonExistingRegister()
        => ExistsIfFalseForNonExistingRegister(CreateAndInitializeRegister());

    [TestMethod]
    public override Task ExistingValueIsNullForNonExistingRegister()
        => ExistingValueIsNullForNonExistingRegister(CreateAndInitializeRegister());

    [TestMethod]
    public override Task DeleteSucceedsForNonExistingRegister()
        => DeleteSucceedsForNonExistingRegister(CreateAndInitializeRegister());

    [TestMethod]
    public override Task DeleteSucceedsForExistingRegister()
        => DeleteSucceedsForExistingRegister(CreateAndInitializeRegister());

    [TestMethod]
    public override Task DeleteSucceedsWithExpectedValueForExistingRegister()
        => DeleteSucceedsWithExpectedValueForExistingRegister(CreateAndInitializeRegister());

    [TestMethod]
    public override Task DeleteFailsWhenNonExpectedValueForExistingRegister()
        => DeleteFailsWhenNonExpectedValueForExistingRegister(CreateAndInitializeRegister());
    
    private async Task<IRegister> CreateAndInitializeRegister([CallerMemberName] string memberName = "")
    {
        var register = new Register(Sql.ConnectionString, tablePrefix: memberName);
        await register.Initialize();
        return register;
    }
}