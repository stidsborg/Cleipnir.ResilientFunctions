using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class MonitorTests : TestTemplates.UtilsTests.MonitorTests
{
    private readonly Dictionary<string, InMemoryMonitor> _monitors = new();
    private readonly object _sync = new();

    [TestMethod]
    public override Task LockCanBeAcquiredAndReleasedSuccessfully()
        => LockCanBeAcquiredAndReleasedSuccessfully(CreateInMemoryMonitor());

    [TestMethod]
    public override Task TwoDifferentLocksCanBeAcquired()
        => TwoDifferentLocksCanBeAcquired(CreateInMemoryMonitor());

    [TestMethod]
    public override Task TakingATakenLockFails()
        => TakingATakenLockFails(CreateInMemoryMonitor());

    [TestMethod]
    public override Task ReTakingATakenLockWithSameKeyIdSucceeds()
        => ReTakingATakenLockWithSameKeyIdSucceeds(CreateInMemoryMonitor());

    [TestMethod]
    public override Task AReleasedLockCanBeTakenAgain()
        => AReleasedLockCanBeTakenAgain(CreateInMemoryMonitor());

    [TestMethod]
    public override Task WaitingAboveThresholdForATakenLockReturnsNull()
        => WaitingAboveThresholdForATakenLockReturnsNull(CreateInMemoryMonitor());

    [TestMethod]
    public override Task WhenALockIsReleasedActiveAcquireShouldGetTheLock()
        => WhenALockIsReleasedActiveAcquireShouldGetTheLock(CreateInMemoryMonitor());

    private Task<IMonitor> CreateInMemoryMonitor([CallerMemberName] string memberName = "")
    {
        var monitor = new InMemoryMonitor();
        lock (_sync)
            _monitors[memberName] = monitor;

        return monitor.CastTo<IMonitor>().ToTask();
    }
    
    protected override Task<int> LockCount(string memberName = "")
    {
        lock (_sync)
            return _monitors[memberName].LockCount.ToTask();
    }
}