namespace Sample.Holion.B.BankTransfer;

public static class Example
{
    public static async Task Perform(Flows flows)
    {
        var transferFlows = flows.TransferFlows;
        var transfer = new Transfer(
            TransferId: Guid.NewGuid(),
            FromAccount: "FAccount",
            FromAccountTransactionId: Guid.NewGuid(),
            ToAccount: "TAccount",
            ToAccountTransactionId: Guid.NewGuid(),
            Amount: 100
        );
        await transferFlows.Run(transfer.TransferId.ToString(), transfer);
    }
}