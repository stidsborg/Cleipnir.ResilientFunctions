using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests;

public abstract class ArbitratorTests
{
    public abstract Task ProposalForNonDecidedGroupOnlyKeySucceeds();
    protected async Task ProposalForNonDecidedGroupOnlyKeySucceeds(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;
        const string proposal = "hello world";
        var groupId = Guid.NewGuid().ToString();
        await arbitrator.Propose(groupId, proposal).ShouldBeTrueAsync();
    }
    
    public abstract Task ProposalForNonDecidedKeySucceeds();
    protected async Task ProposalForNonDecidedKeySucceeds(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;
        const string proposal = "hello world";
        var groupId = Guid.NewGuid().ToString();
        var instanceId = Guid.NewGuid().ToString();
        await arbitrator.Propose(groupId, instanceId, proposal).ShouldBeTrueAsync();
    }
    
    public abstract Task DifferentProposalForDecidedGroupOnlyKeyFails();
    protected async Task DifferentProposalForDecidedGroupOnlyKeyFails(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;
        const string acceptedProposal = "hello world";
        const string deniedProposal = "hello universe";
        var groupId = Guid.NewGuid().ToString();
        await arbitrator.Propose(groupId, acceptedProposal).ShouldBeTrueAsync();

        await arbitrator.Propose(groupId, deniedProposal).ShouldBeFalseAsync();
    }
    
    public abstract Task DifferentProposalForDecidedKeyFails();
    protected async Task DifferentProposalForDecidedKeyFails(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;
        const string proposal = "hello world";
        const string deniedProposal = "hello universe";
        
        var groupId = Guid.NewGuid().ToString();
        var instanceId = Guid.NewGuid().ToString();
        await arbitrator.Propose(groupId, instanceId, proposal).ShouldBeTrueAsync();
        
        await arbitrator.Propose(groupId, instanceId, deniedProposal).ShouldBeFalseAsync();
    }
    
    public abstract Task SameProposalAsDecidedGroupOnlyKeySucceeds();
    protected async Task SameProposalAsDecidedGroupOnlyKeySucceeds(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;
        const string proposal = "hello world";
        var groupId = Guid.NewGuid().ToString();
        await arbitrator.Propose(groupId, proposal).ShouldBeTrueAsync();

        await arbitrator.Propose(groupId, proposal).ShouldBeTrueAsync();
    }
    
    public abstract Task SameProposalAsDecidedSucceeds();
    protected async Task SameProposalAsDecidedSucceeds(Task<IArbitrator> arbitratorTask)
    {
        var arbitrator = await arbitratorTask;
        const string proposal = "hello world";

        var groupId = Guid.NewGuid().ToString();
        var instanceId = Guid.NewGuid().ToString();
        await arbitrator.Propose(groupId, instanceId, proposal).ShouldBeTrueAsync();
        
        await arbitrator.Propose(groupId, instanceId, proposal).ShouldBeTrueAsync();
    }
}