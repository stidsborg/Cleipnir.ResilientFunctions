using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.UtilsTests;

[TestClass]
public class ArbitratorTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests.ArbitratorTests
{
    [TestMethod]
    public override Task ProposalForNonDecidedGroupOnlyKeySucceeds()
        => ProposalForNonDecidedGroupOnlyKeySucceeds(CreateInMemoryArbitrator());

    [TestMethod]
    public override Task ProposalForNonDecidedKeySucceeds() 
        => ProposalForNonDecidedKeySucceeds(CreateInMemoryArbitrator());

    [TestMethod]
    public override Task DifferentProposalForDecidedGroupOnlyKeyFails() 
        => DifferentProposalForDecidedGroupOnlyKeyFails(CreateInMemoryArbitrator());

    [TestMethod]
    public override Task DifferentProposalForDecidedKeyFails() 
        => DifferentProposalForDecidedKeyFails(CreateInMemoryArbitrator());

    [TestMethod]
    public override Task SameProposalAsDecidedGroupOnlyKeySucceeds() 
        => SameProposalAsDecidedGroupOnlyKeySucceeds(CreateInMemoryArbitrator());

    [TestMethod]
    public override Task SameProposalAsDecidedSucceeds() 
        => SameProposalAsDecidedSucceeds(CreateInMemoryArbitrator());

    [TestMethod]
    public override Task DifferentProposalCanBeDecidedAfterDeletion()
        => DifferentProposalCanBeDecidedAfterDeletion(CreateInMemoryArbitrator());

    private Task<IArbitrator> CreateInMemoryArbitrator() => new InMemoryArbitrator().CastTo<IArbitrator>().ToTask();
}