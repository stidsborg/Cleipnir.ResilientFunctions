﻿using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.SqlServer.Utils;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.UtilTests;

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
        var monitor = (Monitor) await CreateAndInitializeMonitor();
        await monitor.Initialize();
    }
    
    private async Task<IMonitor> CreateAndInitializeMonitor([CallerMemberName] string memberName = "")
    {
        var monitor = new Monitor(Sql.ConnectionString, tablePrefix: memberName);
        await monitor.Initialize();
        return monitor;
    }

    protected override async Task<int> LockCount([CallerMemberName] string memberName = "")
    {
        await using var conn = new SqlConnection(Sql.ConnectionString);
        await conn.OpenAsync();
        return await conn.ExecuteScalarAsync<int>($"SELECT COUNT(*) FROM {memberName}RFunctions_Monitor");
    }
}