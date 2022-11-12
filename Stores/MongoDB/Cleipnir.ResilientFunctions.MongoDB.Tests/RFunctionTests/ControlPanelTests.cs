using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.MongoDB.Tests.RFunctionTests;

[TestClass]
public class ControlPanelTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ControlPanelTests
{
    [TestMethod]
    public override Task ExistingActionCanBeDeletedFromControlPanel()
        => ExistingActionCanBeDeletedFromControlPanel(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task ExistingFunctionCanBeDeletedFromControlPanel()
        => ExistingFunctionCanBeDeletedFromControlPanel(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task DeletingExistingActionWithHigherEpochReturnsFalse()
        => DeletingExistingActionWithHigherEpochReturnsFalse(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task DeletingExistingFuncWithHigherEpochReturnsFalse()
        => DeletingExistingFuncWithHigherEpochReturnsFalse(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponingExistingActionFromControlPanelSucceeds()
        => PostponingExistingActionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task PostponingExistingFunctionFromControlPanelSucceeds()
        => PostponingExistingFunctionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FailingExistingActionFromControlPanelSucceeds()
        => FailingExistingActionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task FailingExistingFunctionFromControlPanelSucceeds()
        => FailingExistingFunctionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task SucceedingExistingActionFromControlPanelSucceeds()
        => SucceedingExistingActionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());
    
    [TestMethod]
    public override Task SucceedingExistingFunctionFromControlPanelSucceeds()
        => SucceedingExistingFunctionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReInvokingExistingActionFromControlPanelSucceeds()
        => ReInvokingExistingActionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReInvokingExistingFunctionFromControlPanelSucceeds()
        => ReinvokingExistingFunctionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingActionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelSucceeds()
        => ScheduleReInvokingExistingFunctionFromControlPanelSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingActionFromControlPanelFailsWhenEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected()
        => ScheduleReInvokingExistingFunctionFromControlPanelFailsWhenEpochIsNotAsExpected(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task WaitingForExistingFunctionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingFunctionFromControlPanelToCompleteSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task WaitingForExistingActionFromControlPanelToCompleteSucceeds()
        => WaitingForExistingActionFromControlPanelToCompleteSucceeds(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRFuncSucceedsAfterSuccessfullySavingParamAndScrapbook(NoSql.AutoCreateAndInitializeStore());

    [TestMethod]
    public override Task ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook()
        => ReInvokeRActionSucceedsAfterSuccessfullySavingParamAndScrapbook(NoSql.AutoCreateAndInitializeStore());
}