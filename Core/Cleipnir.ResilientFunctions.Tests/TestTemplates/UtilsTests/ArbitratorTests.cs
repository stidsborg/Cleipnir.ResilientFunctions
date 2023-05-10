using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests;

public abstract class ArbitratorTests
{
    public abstract Task ProposalForNonDecidedKeySucceeds();
    protected async Task ProposalForNonDecidedKeySucceeds(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;
        const string proposal = "hello world";
        var group = Guid.NewGuid().ToString();
        var name = Guid.NewGuid().ToString();
        await arbitrator.Propose(group, name, proposal).ShouldBeTrueAsync();
    }

    public abstract Task DifferentProposalForDecidedKeyFails();
    protected async Task DifferentProposalForDecidedKeyFails(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;
        const string proposal = "hello world";
        const string deniedProposal = "hello universe";
        
        var group = Guid.NewGuid().ToString();
        var name = Guid.NewGuid().ToString();
        await arbitrator.Propose(group, name, proposal).ShouldBeTrueAsync();
        
        await arbitrator.Propose(group, name, deniedProposal).ShouldBeFalseAsync();
    }
    
    public abstract Task SameProposalAsDecidedSucceeds();
    protected async Task SameProposalAsDecidedSucceeds(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;
        const string proposal = "hello world";

        var groupId = Guid.NewGuid().ToString();
        var name = Guid.NewGuid().ToString();
        await arbitrator.Propose(groupId, name, proposal).ShouldBeTrueAsync();
        
        await arbitrator.Propose(groupId, name, proposal).ShouldBeTrueAsync();
    }
    
    public abstract Task DifferentProposalCanBeDecidedAfterDeletion();
    protected async Task DifferentProposalCanBeDecidedAfterDeletion(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;

        var groupId = Guid.NewGuid().ToString();
        var instanceId = Guid.NewGuid().ToString();
        await arbitrator.Propose(groupId, instanceId, "proposal1").ShouldBeTrueAsync();
        await arbitrator.Delete(groupId, instanceId);
        await arbitrator.Propose(groupId, instanceId, "proposal2").ShouldBeTrueAsync();
    }
}