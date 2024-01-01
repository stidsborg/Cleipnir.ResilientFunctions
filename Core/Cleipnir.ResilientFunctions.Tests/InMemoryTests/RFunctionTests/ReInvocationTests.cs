﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ReInvocationTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ReInvocationTests
{
    [TestMethod]
    public override Task ActionReInvocationSunshineScenario()
        => ActionReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task ActionWithScrapbookReInvocationSunshineScenario()
        => ActionWithScrapbookReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task UpdatedParameterIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterIsPassedInOnReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario()
        => UpdatedParameterAndScrapbookIsPassedInOnReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnAction()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnAction(CreateInMemoryStore());
    [TestMethod]
    public override Task ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc()
        => ScrapbookUpdaterIsCalledBeforeReInvokeOnFunc(CreateInMemoryStore());
    [TestMethod]
    public override Task FuncReInvocationSunshineScenario()
        => FuncReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task FuncWithScrapbookReInvocationSunshineScenario()
        => FuncWithScrapbookReInvocationSunshineScenario(CreateInMemoryStore());
    [TestMethod]
    public override Task ReInvocationFailsWhenTheFunctionDoesNotExist()
        => ReInvocationFailsWhenTheFunctionDoesNotExist(CreateInMemoryStore());

    private Task<IFunctionStore> CreateInMemoryStore() 
        => FunctionStoreFactory.Create();
}