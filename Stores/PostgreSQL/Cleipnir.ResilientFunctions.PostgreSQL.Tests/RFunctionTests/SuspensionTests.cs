using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class SuspensionTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.SuspensionTests
{
    [TestMethod]
    public override Task ActionCanBeSuspended()
        => ActionCanBeSuspended(Sql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FunctionCanBeSuspended()
        => FunctionCanBeSuspended(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded()
        => DetectionOfEligibleSuspendedFunctionSucceedsAfterEventAdded(Sql.AutoCreateAndInitializeStore());
}