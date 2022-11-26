using System.Runtime.CompilerServices;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL.Tests.UtilTests;

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

    [TestMethod]
    public async Task InvokingInitializeTwiceSucceeds()
    {
        var monitor = (Cleipnir.ResilientFunctions.MySQL.Utils.Monitor) await CreateAndInitializeMonitor();
        await monitor.Initialize();
    }
    
    private async Task<IMonitor> CreateAndInitializeMonitor([CallerMemberName] string memberName = "")
    {
        var monitor = new Cleipnir.ResilientFunctions.MySQL.Utils.Monitor(Sql.ConnectionString, tablePrefix: memberName);
        await monitor.Initialize();
        return monitor;
    }

    protected override async Task<int> LockCount([CallerMemberName] string memberName = "")
    {
        await using var conn = await Sql.ConnFunc();
        var sql = $"SELECT COUNT(*) FROM {memberName.ToLower()}monitor";
        await using var command = new MySqlCommand(sql, conn);
        var count = (int) (long) (await command.ExecuteScalarAsync() ?? 0);
        return count;
    }
}