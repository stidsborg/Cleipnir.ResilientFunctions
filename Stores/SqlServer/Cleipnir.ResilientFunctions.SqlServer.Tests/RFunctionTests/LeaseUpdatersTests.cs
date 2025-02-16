﻿using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.RFunctionTests;

[TestClass]
public class LeaseUpdatersTests : ResilientFunctions.Tests.TestTemplates.RFunctionTests.LeaseUpdatersForLeaseLengthTests
{
    [TestMethod]
    public override Task LeaseUpdaterUpdatesExpiryForEligibleFlows()
        => LeaseUpdaterUpdatesExpiryForEligibleFlows(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task LeaseUpdatersRefreshedCorrectlyOnUnexpectedNumberOfAffectedFlows()
        => LeaseUpdatersRefreshedCorrectlyOnUnexpectedNumberOfAffectedFlows(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task LeaseUpdatersRepositoryThrowsResultsInUnhandledException()
        => LeaseUpdatersRepositoryThrowsResultsInUnhandledException(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task RunningLeaseUpdatersCanBeDisposed()
        => RunningLeaseUpdatersCanBeDisposed(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task FilterOutContainsFiltersOutActiveFlows()
        => FilterOutContainsFiltersOutActiveFlows(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task FilterOutContainsReturnsSameCollectionUnmodifiedWhenNoFilterIsPerformed()
        => FilterOutContainsReturnsSameCollectionUnmodifiedWhenNoFilterIsPerformed(FunctionStoreFactory.Create());
    
    [TestMethod]
    public override Task LeaseUpdatersFiltersOutAlreadyContains()
        => LeaseUpdatersFiltersOutAlreadyContains(FunctionStoreFactory.Create());

    [TestMethod]
    public override Task LeaseUpdatersReturnsSameReferenceWhenFiltersWhenThereAreNoAlreadyContains()
        => LeaseUpdatersReturnsSameReferenceWhenFiltersWhenThereAreNoAlreadyContains(FunctionStoreFactory.Create());
}