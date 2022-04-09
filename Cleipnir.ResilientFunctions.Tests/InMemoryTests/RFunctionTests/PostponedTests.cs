using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class PostponedTests : TestTemplates.RFunctionTests.PostponedTests
{
    [TestMethod]
    public override Task PostponedFuncIsCompletedByWatchDog()
        => PostponedFuncIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponedFuncWithScrapbookIsCompletedByWatchDog()
        => PostponedFuncWithScrapbookIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponedActionIsCompletedByWatchDog()
        => PostponedActionIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task PostponedActionWithScrapbookIsCompletedByWatchDog()
        => PostponedActionWithScrapbookIsCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task InMemoryPostponedActionIsNotCompletedByWatchDog()
        => InMemoryPostponedActionIsNotCompletedByWatchDog(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    public override Task ImplicitlyPostponedFunctionAboveTwoSecondsIsNotPostponedInMemory()
        => ImplicitlyPostponedFunctionAboveTwoSecondsIsNotPostponedInMemory(
            new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()
        );
}