using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySqlConnector;
using Monitor = Cleipnir.ResilientFunctions.Utils.Monitor.Monitor;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests.UtilTests;

[TestClass]
public class MonitorTests : ResilientFunctions.Tests.TestTemplates.UtilsTests.MonitorTests
{
    [TestMethod]
    public override Task LockCanBeAcquiredAndReleasedSuccessfully()
        => LockCanBeAcquiredAndReleasedSuccessfully(CreateAndInitializeMonitor());

    [TestMethod]
    public override Task TwoDifferentLocksCanBeAcquired()
        => TwoDifferentLocksCanBeAcquired(CreateAndInitializeMonitor());

    [TestMethod]
    public override Task TakingATakenLockFails()
        => TakingATakenLockFails(CreateAndInitializeMonitor());

    [TestMethod]
    public override Task ReTakingATakenLockWithSameKeyIdSucceeds()
        => ReTakingATakenLockWithSameKeyIdSucceeds(CreateAndInitializeMonitor());

    [TestMethod]
    public override Task AReleasedLockCanBeTakenAgain()
        => AReleasedLockCanBeTakenAgain(CreateAndInitializeMonitor());

    [TestMethod]
    public override Task WaitingAboveThresholdForATakenLockReturnsNull()
        => WaitingAboveThresholdForATakenLockReturnsNull(CreateAndInitializeMonitor());

    [TestMethod]
    public override Task WhenALockIsReleasedActiveAcquireShouldGetTheLock()
        => WhenALockIsReleasedActiveAcquireShouldGetTheLock(CreateAndInitializeMonitor());

    private async Task<IMonitor> CreateAndInitializeMonitor([CallerMemberName] string memberName = "")
    {
        var underlyingRegister = new MariaDbUnderlyingRegister(Sql.ConnectionString);
        var monitor = new Monitor(underlyingRegister);
        await underlyingRegister.Initialize();
        return monitor;
    }
}