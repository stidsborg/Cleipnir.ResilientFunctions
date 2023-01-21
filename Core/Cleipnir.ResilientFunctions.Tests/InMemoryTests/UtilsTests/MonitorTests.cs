﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.UtilsTests;

[TestClass]
public class MonitorTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests.MonitorTests
{
    private InMemoryMonitor _monitor = new();

    [TestInitialize]
    public void Initialize() => _monitor = new InMemoryMonitor();

    [TestMethod]
    public override Task LockCanBeAcquiredAndReleasedSuccessfully()
        => LockCanBeAcquiredAndReleasedSuccessfully(_monitor.CastTo<IMonitor>().ToTask());

    [TestMethod]
    public override Task TwoDifferentLocksCanBeAcquired()
        => TwoDifferentLocksCanBeAcquired(_monitor.CastTo<IMonitor>().ToTask());

    [TestMethod]
    public override Task TakingATakenLockFails()
        => TakingATakenLockFails(_monitor.CastTo<IMonitor>().ToTask());

    [TestMethod]
    public override Task ReTakingATakenLockWithSameKeyIdSucceeds()
        => ReTakingATakenLockWithSameKeyIdSucceeds(_monitor.CastTo<IMonitor>().ToTask());

    [TestMethod]
    public override Task AReleasedLockCanBeTakenAgain()
        => AReleasedLockCanBeTakenAgain(_monitor.CastTo<IMonitor>().ToTask());

    [TestMethod]
    public override Task WaitingAboveThresholdForATakenLockReturnsNull()
        => WaitingAboveThresholdForATakenLockReturnsNull(_monitor.CastTo<IMonitor>().ToTask());

    [TestMethod]
    public override Task WhenALockIsReleasedActiveAcquireShouldGetTheLock()
        => WhenALockIsReleasedActiveAcquireShouldGetTheLock(_monitor.CastTo<IMonitor>().ToTask());

    protected override Task<int> LockCount(string memberName = "") => _monitor.LockCount.ToTask();
}