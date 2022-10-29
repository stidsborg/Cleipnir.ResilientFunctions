using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class ControlPanelTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests.ControlPanelTests
{
    [TestMethod]
    public override Task ExistingActionCanBeDeletedFromControlPanel()
        => ExistingActionCanBeDeletedFromControlPanel(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task ExistingFunctionCanBeDeletedFromControlPanel()
        => ExistingFunctionCanBeDeletedFromControlPanel(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task DeletingExistingActionWithHigherEpochReturnsFalse()
        => DeletingExistingActionWithHigherEpochReturnsFalse(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task DeletingExistingFuncWithHigherEpochReturnsFalse()
        => DeletingExistingFuncWithHigherEpochReturnsFalse(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task PostponingExistingActionFromControlPanelSucceeds()
        => PostponingExistingActionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task PostponingExistingFunctionFromControlPanelSucceeds()
        => PostponingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task FailingExistingActionFromControlPanelSucceeds()
        => FailingExistingActionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task FailingExistingFunctionFromControlPanelSucceeds()
        => FailingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task SucceedingExistingActionFromControlPanelSucceeds()
        => SucceedingExistingActionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());

    [TestMethod]
    public override Task SucceedingExistingFunctionFromControlPanelSucceeds()
        => SucceedingExistingFunctionFromControlPanelSucceeds(Utils.CreateInMemoryFunctionStoreTask());
}