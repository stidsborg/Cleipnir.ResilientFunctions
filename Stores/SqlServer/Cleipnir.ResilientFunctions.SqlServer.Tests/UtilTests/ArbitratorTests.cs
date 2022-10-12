using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.SqlServer.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.UtilTests;

[TestClass]
public class ArbitratorTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests.ArbitratorTests
{
    [TestMethod]
    public override Task ProposalForNonDecidedGroupOnlyKeySucceeds()
        => ProposalForNonDecidedGroupOnlyKeySucceeds(CreateArbitrator());

    [TestMethod]
    public override Task ProposalForNonDecidedKeySucceeds() 
        => ProposalForNonDecidedKeySucceeds(CreateArbitrator());

    [TestMethod]
    public override Task DifferentProposalForDecidedGroupOnlyKeyFails() 
        => DifferentProposalForDecidedGroupOnlyKeyFails(CreateArbitrator());

    [TestMethod]
    public override Task DifferentProposalForDecidedKeyFails() 
        => DifferentProposalForDecidedKeyFails(CreateArbitrator());

    [TestMethod]
    public override Task SameProposalAsDecidedGroupOnlyKeySucceeds() 
        => SameProposalAsDecidedGroupOnlyKeySucceeds(CreateArbitrator());

    [TestMethod]
    public override Task SameProposalAsDecidedSucceeds() 
        => SameProposalAsDecidedSucceeds(CreateArbitrator());

    [TestMethod]
    public override Task DifferentProposalCanBeDecidedAfterDeletion()
        => DifferentProposalCanBeDecidedAfterDeletion(CreateArbitrator());

    [TestMethod]
    public async Task InvokingInitializeTwiceSucceeds()
    {
        var arbitrator = (Arbitrator) await CreateArbitrator();
        await arbitrator.Initialize();
    }
    
    private async Task<IArbitrator> CreateArbitrator([CallerMemberName] string memberName = "")
    {
        var arbitrator = new Arbitrator(Sql.ConnectionString, tablePrefix: memberName);
        await arbitrator.Initialize();
        return arbitrator;
    }
}