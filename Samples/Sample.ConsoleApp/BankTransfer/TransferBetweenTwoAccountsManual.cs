using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.BankTransfer;

public sealed class TransferSaga
{
    private readonly RAction<Transfer, RScrapbook> _rAction;
    public TransferSaga(RFunctions rFunctions)
    {
        _rAction = rFunctions
            .RegisterMethod<Inner>()
            .RegisterAction<Transfer, RScrapbook>(
                functionTypeId: nameof(TransferSaga),
                inner => inner.Perform
            );
    }

    public Task Perform(Transfer transfer)
        => _rAction.Invoke(transfer.TransferId.ToString(), transfer);

    public class Inner
    {
        private IBankClient BankClient { get; }
        
        public Inner(IBankClient bankClient) => BankClient = bankClient;

        public async Task Perform(Transfer transfer, RScrapbook scrapbook)
        {
            var deductTask = scrapbook.DoAtMostOnce(
                "DeductAmount",
                () => BankClient.PostTransaction(transfer.FromAccountTransactionId, transfer.FromAccount, -transfer.Amount)
            );
            
            var addTask = scrapbook.DoAtMostOnce(
                "AddAmount",
                () => BankClient.PostTransaction(transfer.ToAccountTransactionId, transfer.ToAccount, transfer.Amount)
            );

            await Task.WhenAll(deductTask, addTask);
        }
    }
}