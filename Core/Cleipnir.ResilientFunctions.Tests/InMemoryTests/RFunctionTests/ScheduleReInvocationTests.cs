﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ScheduleReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ScheduleReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task ActionWithStateReInvocationSunshineScenario()
        => ActionWithStateReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task FuncWithStateReInvocationSunshineScenario()
        => FuncWithStateReInvocationSunshineScenario(CreateInMemoryStore());

    private Task<IFunctionStore> CreateInMemoryStore() 
        => FunctionStoreFactory.Create();
}