﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

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