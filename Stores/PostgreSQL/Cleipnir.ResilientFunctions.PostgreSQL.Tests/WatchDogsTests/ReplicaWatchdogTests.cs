using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests.WatchDogsTests;

[TestClass]
public class ReplicaWatchdogTests : ResilientFunctions.Tests.TestTemplates.WatchDogsTests.ReplicaWatchdogTests
{
    [TestMethod]
    public override Task SunshineScenario()
        => SunshineScenario(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task ReplicaWatchdogStartResultsInAddedReplicaInStore()
        => ReplicaWatchdogStartResultsInAddedReplicaInStore(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task StrikedOutReplicaIsRemovedFromStore()
        => StrikedOutReplicaIsRemovedFromStore(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task RunningWatchdogUpdatesItsOwnHeartbeat()
        => RunningWatchdogUpdatesItsOwnHeartbeat(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task ReplicaIdOffsetIfCalculatedCorrectly()
        => ReplicaIdOffsetIfCalculatedCorrectly(FunctionStoreFactory.Create());
 
    [TestMethod]
    public override Task NonExistingReplicaIdOffsetIsNull()
        => NonExistingReplicaIdOffsetIsNull(FunctionStoreFactory.Create());   
}