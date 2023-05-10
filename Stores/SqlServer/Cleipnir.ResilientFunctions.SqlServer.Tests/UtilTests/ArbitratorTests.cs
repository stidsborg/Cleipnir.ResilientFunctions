﻿using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Utils;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests.UtilTests;

[TestClass]
public class ArbitratorTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.UtilsTests.ArbitratorTests
{
    [TestMethod]
    public override Task ProposalForNonDecidedKeySucceeds() 
        => ProposalForNonDecidedKeySucceeds(CreateArbitrator());

    [TestMethod]
    public override Task DifferentProposalForDecidedKeyFails() 
        => DifferentProposalForDecidedKeyFails(CreateArbitrator());

    [TestMethod]
    public override Task SameProposalAsDecidedSucceeds() 
        => SameProposalAsDecidedSucceeds(CreateArbitrator());

    [TestMethod]
    public override Task DifferentProposalCanBeDecidedAfterDeletion()
        => DifferentProposalCanBeDecidedAfterDeletion(CreateArbitrator());

    private async Task<IArbitrator> CreateArbitrator([CallerMemberName] string memberName = "")
    {
        var underlyingRegister = new SqlServerUnderlyingRegister(Sql.ConnectionString, tablePrefix: memberName);
        var arbitrator = new Arbitrator(underlyingRegister);
        await underlyingRegister.Initialize();
        return arbitrator;
    }
}