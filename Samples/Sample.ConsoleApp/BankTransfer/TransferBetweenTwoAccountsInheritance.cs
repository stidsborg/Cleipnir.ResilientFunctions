using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.BankTransfer;

public class TransferBetweenTwoAccountsInheritance : RSaga<Transfer>
{
    private IBankClient BankClient { get; }

    public TransferBetweenTwoAccountsInheritance(RFunctions rFunctions, IBankClient bankClient) : base(
        "Transfers".ToFunctionTypeId(),
        accounts => $"{accounts.FromAccount}¤{accounts.ToAccount}",
        rFunctions
    ) => BankClient = bankClient;
    
    protected override async Task<RResult> Func(Transfer transfer)
    {
        try
        {
            await BankClient.PostTransaction(transfer.FromAccountTransactionId, transfer.FromAccount, -transfer.Amount);
            await BankClient.PostTransaction(transfer.ToAccountTransactionId, transfer.ToAccount, transfer.Amount);
            return RResult.Success;
        }
        catch (Exception exception)
        {
            return Fail.WithException(exception);
        }
    }
}

public record Transfer(
    string FromAccount,
    Guid FromAccountTransactionId,
    string ToAccount, 
    Guid ToAccountTransactionId,
    decimal Amount
);
