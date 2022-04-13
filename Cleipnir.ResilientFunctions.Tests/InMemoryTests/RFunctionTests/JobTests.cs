using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class JobTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.JobTests
{
    [TestMethod]
    public override Task JobCanBeRetried()
        => JobCanBeRetried(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask()); 

    [TestMethod]
    public override Task JobCanBeStartedMultipleTimesWithoutError()
        => JobCanBeStartedMultipleTimesWithoutError(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());

    [TestMethod]
    public override Task CrashedJobIsRetried()
        => CrashedJobIsRetried(new InMemoryFunctionStore().CastTo<IFunctionStore>().ToTask());
}