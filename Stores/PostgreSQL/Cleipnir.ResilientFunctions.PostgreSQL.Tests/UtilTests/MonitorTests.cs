﻿using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.UtilTests;

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
        var underlyingRegister = new PostgresSqlUnderlyingRegister(Sql.ConnectionString);
        var monitor = new Monitor(underlyingRegister);
        await underlyingRegister.Initialize();
        await underlyingRegister.Initialize();
        return monitor;
    }
}