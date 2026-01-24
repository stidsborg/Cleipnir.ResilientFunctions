using System;
using System.Reflection;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class SettingsTests
{
    [TestMethod]
    public void ReplicaHeartbeatFrequencySettingIsUsedInFunctionsRegistry()
    {
        var heartbeatFrequency = TimeSpan.FromSeconds(123);
        var settings = new Settings(replicaHeartbeatFrequency: heartbeatFrequency);
        var functionStore = new InMemoryFunctionStore();
        
        var functionsRegistry = new FunctionsRegistry(functionStore, settings);

        var replicaWatchdog = (CoreRuntime.Watchdogs.ReplicaWatchdog) typeof(FunctionsRegistry)
            .GetField("_replicaWatchdog", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(functionsRegistry)!;

        replicaWatchdog.HeartbeatFrequency.ShouldBe(heartbeatFrequency);
    }
    
    [TestMethod]
    public void NegativeReplicaHeartbeatFrequencyThrowsArgumentOutOfRangeException()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => new Settings(replicaHeartbeatFrequency: TimeSpan.FromSeconds(-1)));
    }
    
    [TestMethod]
    public void ZeroReplicaHeartbeatFrequencyDoesNotThrowArgumentOutOfRangeException()
    {
        _ = new Settings(replicaHeartbeatFrequency: TimeSpan.Zero);
    }
}
