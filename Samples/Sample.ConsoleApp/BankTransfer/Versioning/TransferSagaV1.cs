using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.BankTransfer.Versioning;

public sealed class TransferSagaV1
{
    private readonly ActionRegistration<Transfer> _actionRegistration;
    public TransferSagaV1(FunctionsRegistry functionsRegistry)
    {
        var inner = new Inner(new BankCentralClient());
        _actionRegistration = functionsRegistry
            .RegisterAction<Transfer>(
                flowType: nameof(TransferSagaV1),
                (transfer, workflow) => inner.Perform(transfer, workflow)
            );
    }

    public Task Perform(Transfer transfer)
        => _actionRegistration.Run(transfer.TransferId.ToString(), transfer);

    public class Inner
    {
        private IBankCentralClient BankCentralClient { get; }

        public Inner(IBankCentralClient bankCentralClient) => BankCentralClient = bankCentralClient;

        public async Task Perform(Transfer transfer, Workflow workflow)
        {
            var deductTask = workflow.Effect.Capture(
                () => BankCentralClient.PostTransaction(transfer.FromAccountTransactionId, transfer.FromAccount, -transfer.Amount)
            );

            var addTask = workflow.Effect.Capture(
                () => BankCentralClient.PostTransaction(transfer.ToAccountTransactionId, transfer.ToAccount, transfer.Amount)
            );

            await Task.WhenAll(deductTask, addTask);
        }
    }
}