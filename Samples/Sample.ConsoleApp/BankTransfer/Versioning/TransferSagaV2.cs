using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.BankTransfer.Versioning;

public sealed class TransferSagaV2
{
    private readonly ActionRegistration<Transfer> _actionRegistration;
    public TransferSagaV2(FunctionsRegistry functionsRegistry)
    {
        var saga = new Inner(new BankCentralClient());
        _actionRegistration = functionsRegistry
            .RegisterAction<Transfer>(
                functionTypeId: nameof(TransferSagaV2),
                (transfer, workflow) => saga.Perform(transfer, workflow)
            );
    }

    public Task Perform(Transfer transfer)
        => _actionRegistration.Invoke(transfer.TransferId.ToString(), transfer);

    public class Inner
    {
        private IBankCentralClient BankCentralClient { get; }
        
        public Inner(IBankCentralClient bankCentralClient) => BankCentralClient = bankCentralClient;

        public async Task Perform(Transfer transfer, Workflow workflow)
        {
            var arbitrator = workflow.Utilities.Arbitrator;
            var (effect, _, _) = workflow;
            var success = await arbitrator.Propose("BankTransfer", transfer.TransferId.ToString(), value: "V1");
            if (!success) throw new InvalidOperationException("Other version was selected for execution");
            
            var deductTask = effect.Capture(
                "DeductAmount",
                () => BankCentralClient.PostTransaction(transfer.FromAccountTransactionId, transfer.FromAccount, -transfer.Amount)
            );
            
            var addTask = effect.Capture(
                "AddAmount",
                () => BankCentralClient.PostTransaction(transfer.ToAccountTransactionId, transfer.ToAccount, transfer.Amount)
            );

            await Task.WhenAll(deductTask, addTask);
        }
    }
}