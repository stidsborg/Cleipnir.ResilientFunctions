using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class MonitorTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.MonitorTests
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

    private async Task<IMonitor> CreateAndInitializeMonitor([System.Runtime.CompilerServices.CallerMemberName] string memberName = "")
    {
        var monitor = new Monitor(Sql.ConnFunc, tablePrefix: memberName);
        await monitor.Initialize();
        return monitor;
    }

    protected override async Task<int> LockCount([System.Runtime.CompilerServices.CallerMemberName] string memberName = "")
    {
        await using var conn = await Sql.ConnFunc();
        return await conn.ExecuteScalarAsync<int>($"SELECT COUNT(*) FROM {memberName}Monitor");
    }
}