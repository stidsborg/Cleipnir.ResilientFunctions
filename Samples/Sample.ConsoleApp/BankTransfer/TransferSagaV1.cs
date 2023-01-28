using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Utils.Register;

namespace ConsoleApp.BankTransfer;

public sealed class TransferSagaV1
{
    private readonly RAction<Transfer, RScrapbook> _rAction;
    public TransferSagaV1(RFunctions rFunctions)
    {
        _rAction = rFunctions
            .RegisterMethod<Inner>()
            .RegisterAction<Transfer, RScrapbook>(
                functionTypeId: nameof(TransferSagaV1),
                inner => inner.Perform
            );
    }

    public Task Perform(Transfer transfer)
        => _rAction.Invoke(transfer.TransferId.ToString(), transfer);

    public class Inner
    {
        private IBankCentralClient BankCentralClient { get; }

        public Inner(IBankCentralClient bankCentralClient) => BankCentralClient = bankCentralClient;

        public async Task Perform(Transfer transfer, RScrapbook scrapbook, Context context)
        {
            var arbitrator = context.Utilities.Arbitrator;
            var success = await arbitrator.Propose("BankTransfer", transfer.TransferId.ToString(), value: "V1");
            if (!success) throw new InvalidOperationException("Other version was selected for execution");
            
            var deductTask = scrapbook.DoAtMostOnce(
                "DeductAmount",
                () => BankCentralClient.PostTransaction(transfer.FromAccountTransactionId, transfer.FromAccount, -transfer.Amount)
            );
            
            var addTask = scrapbook.DoAtMostOnce(
                "AddAmount",
                () => BankCentralClient.PostTransaction(transfer.ToAccountTransactionId, transfer.ToAccount, transfer.Amount)
            );

            await Task.WhenAll(deductTask, addTask);
        }
    }
}