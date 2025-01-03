﻿using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.MariaDb.Tests;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests;

[TestClass]
public class SemaphoreStoreTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.SemaphoreStoreTests
{
    [TestMethod]
    public override Task SunshineScenarioTest()
        => SunshineScenarioTest(FunctionStoreFactory.Create().SelectAsync(s => s.SemaphoreStore));
    
    [TestMethod]
    public override Task ReleasingSemaphoreTwiceSucceeds()
        => ReleasingSemaphoreTwiceSucceeds(FunctionStoreFactory.Create().SelectAsync(s => s.SemaphoreStore));
    
    [TestMethod]
    public override Task AcquiringTheSameSemaphoreTwiceIsIdempotent()
        => AcquiringTheSameSemaphoreTwiceIsIdempotent(FunctionStoreFactory.Create().SelectAsync(s => s.SemaphoreStore));
}