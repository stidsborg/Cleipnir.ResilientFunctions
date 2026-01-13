using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class WorkflowMessageTests : ResilientFunctions.Tests.TestTemplates.FunctionTests.WorkflowMessageTests
{
    [TestMethod]
    public override Task WorkflowMessagePullsMessageSuccessfully()
        => WorkflowMessagePullsMessageSuccessfully(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessagePullsFirstMessageWhenMultipleMessagesExist()
        => WorkflowMessagePullsFirstMessageWhenMultipleMessagesExist(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithDateTimeReturnsMessageBeforeTimeout()
        => WorkflowMessageWithDateTimeReturnsMessageBeforeTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithDateTimeReturnsNullOnTimeout()
        => WorkflowMessageWithDateTimeReturnsNullOnTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithTimeSpanReturnsMessageBeforeTimeout()
        => WorkflowMessageWithTimeSpanReturnsMessageBeforeTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithTimeSpanReturnsNullOnTimeout()
        => WorkflowMessageWithTimeSpanReturnsNullOnTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithFilterReturnsMatchingMessage()
        => WorkflowMessageWithFilterReturnsMatchingMessage(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithFilterIgnoresNonMatchingMessages()
        => WorkflowMessageWithFilterIgnoresNonMatchingMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithFilterAndDateTimeReturnsMatchingMessageBeforeTimeout()
        => WorkflowMessageWithFilterAndDateTimeReturnsMatchingMessageBeforeTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithFilterAndDateTimeReturnsNullOnTimeout()
        => WorkflowMessageWithFilterAndDateTimeReturnsNullOnTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithFilterAndTimeSpanReturnsMatchingMessageBeforeTimeout()
        => WorkflowMessageWithFilterAndTimeSpanReturnsMatchingMessageBeforeTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithFilterAndTimeSpanReturnsNullOnTimeout()
        => WorkflowMessageWithFilterAndTimeSpanReturnsNullOnTimeout(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageIsIdempotentAcrossRestarts()
        => WorkflowMessageIsIdempotentAcrossRestarts(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task MultipleSequentialMessageCallsReturnDifferentMessages()
        => MultipleSequentialMessageCallsReturnDifferentMessages(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageCanPullDifferentMessageTypes()
        => WorkflowMessageCanPullDifferentMessageTypes(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessagePullAsObjectReturnsCorrectDeserializedType()
        => WorkflowMessagePullAsObjectReturnsCorrectDeserializedType(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task WorkflowMessageWithMaxWaitSuspendsFlowWhenExpired()
        => WorkflowMessageWithMaxWaitSuspendsFlowWhenExpired(FunctionStoreFactory.Create());
}
