﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class ActivityStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.ActivityStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(FunctionStoreFactory.Create().SelectAsync(f => f.ActivityStore));
}