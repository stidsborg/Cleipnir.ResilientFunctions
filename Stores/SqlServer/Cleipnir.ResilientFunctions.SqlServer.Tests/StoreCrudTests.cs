﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class StoreCrudTests : ResilientFunctions.Tests.TestTemplates.StoreCrudTests
{
    [TestMethod]
    public override Task FunctionCanBeCreatedWithASingleParameterSuccessfully()
        => FunctionCanBeCreatedWithASingleParameterSuccessfully(FunctionStoreFactory.Create());
        
    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully()
        => FunctionCanBeCreatedWithTwoParametersAndScrapbookSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task FetchingNonExistingFunctionReturnsNull()
        => FetchingNonExistingFunctionReturnsNull(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SignOfLifeIsUpdatedWhenCurrentEpochMatches()
        => SignOfLifeIsUpdatedWhenCurrentEpochMatches(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent()
        => SignOfLifeIsNotUpdatedWhenCurrentEpochIsDifferent(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task UpdateScrapbookSunshineScenario()
        => UpdateScrapbookSunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected()
        => ScrapbookUpdateFailsWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingFunctionCanBeDeleted()
        => ExistingFunctionCanBeDeleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task NonExistingFunctionCanBeDeleted()
        => NonExistingFunctionCanBeDeleted(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected()
        => ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParameterAndScrapbookCanBeUpdatedOnExistingFunction()
        => ParameterAndScrapbookCanBeUpdatedOnExistingFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParameterCanBeUpdatedOnExistingFunction()
        => ParameterCanBeUpdatedOnExistingFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ScrapbookCanBeUpdatedOnExistingFunction()
        => ScrapbookCanBeUpdatedOnExistingFunction(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ParameterAndScrapbookAreNotUpdatedWhenEpochDoesNotMatch()
        => ParameterAndScrapbookAreNotUpdatedWhenEpochDoesNotMatch(FunctionStoreFactory.Create());
}