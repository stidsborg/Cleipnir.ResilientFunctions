using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.RFunctionTests;

[TestClass]
public class MessagingTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.MessagingTests
{
    [TestMethod]
    public override Task FunctionCompletesAfterAwaitedMessageIsReceived()
        => FunctionCompletesAfterAwaitedMessageIsReceived(Sql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist()
        => FunctionIsSuspendedWhenAwaitedMessageDoesNotAlreadyExist(Sql.AutoCreateAndInitializeStore());
}