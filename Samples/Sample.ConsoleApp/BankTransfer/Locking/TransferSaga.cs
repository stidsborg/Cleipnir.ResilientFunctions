using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using ConsoleApp.BankTransfer.Versioning;

namespace ConsoleApp.BankTransfer.Locking;

public sealed class TransferSaga
{
    private readonly RAction<Transfer> _rAction;
    public TransferSaga(RFunctions rFunctions)
    {
        var inner = new Inner(new BankCentralClient());
        _rAction = rFunctions
            .RegisterAction<Transfer>(
                functionTypeId: nameof(TransferSaga).ToFunctionTypeId(),
                (transfer, context) => inner.Perform(transfer, context)
            );
    }

    public Task Perform(Transfer transfer)
        => _rAction.Invoke(transfer.TransferId.ToString(), transfer);

    private class Inner
    {
        private IBankCentralClient BankCentralClient { get; }

        public Inner(IBankCentralClient bankCentralClient) => BankCentralClient = bankCentralClient;

        public async Task Perform(Transfer transfer, Context context)
        {
            var monitor = context.Utilities.Monitor;
            var lockId = await context.Activity.Do("lockId", () => Guid.NewGuid().ToString());
            await using var _ = await monitor.Acquire(
                new LockInfo("Account", transfer.FromAccount, lockId),
                new LockInfo("Account", transfer.ToAccount, lockId)
            );
            
            var deductTask = context.Activity.Do(
                "DeductAmount",
                () => BankCentralClient.PostTransaction(transfer.FromAccountTransactionId, transfer.FromAccount, -transfer.Amount)
            );
            
            var addTask = context.Activity.Do(
                "AddAmount",
                () => BankCentralClient.PostTransaction(transfer.ToAccountTransactionId, transfer.ToAccount, transfer.Amount)
            );

            await Task.WhenAll(deductTask, addTask);
        }
    }
}