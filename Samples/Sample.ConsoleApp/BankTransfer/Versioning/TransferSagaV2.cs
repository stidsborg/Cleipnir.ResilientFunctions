﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.BankTransfer.Versioning;

public sealed class TransferSagaV2
{
    private readonly RAction<Transfer, RScrapbook> _rAction;
    public TransferSagaV2(FunctionsRegistry functionsRegistry)
    {
        var saga = new Inner(new BankCentralClient());
        _rAction = functionsRegistry
            .RegisterAction<Transfer, RScrapbook>(
                functionTypeId: nameof(TransferSagaV2),
                (transfer, scrapbook, workflow) => saga.Perform(transfer, scrapbook, workflow)
            );
    }

    public Task Perform(Transfer transfer)
        => _rAction.Invoke(transfer.TransferId.ToString(), transfer);

    public class Inner
    {
        private IBankCentralClient BankCentralClient { get; }
        
        public Inner(IBankCentralClient bankCentralClient) => BankCentralClient = bankCentralClient;

        public async Task Perform(Transfer transfer, RScrapbook scrapbook, Workflow workflow)
        {
            var arbitrator = workflow.Utilities.Arbitrator;
            var (activity, messages) = workflow;
            var success = await arbitrator.Propose("BankTransfer", transfer.TransferId.ToString(), value: "V1");
            if (!success) throw new InvalidOperationException("Other version was selected for execution");
            
            var deductTask = activity.Do(
                "DeductAmount",
                () => BankCentralClient.PostTransaction(transfer.FromAccountTransactionId, transfer.FromAccount, -transfer.Amount)
            );
            
            var addTask = activity.Do(
                "AddAmount",
                () => BankCentralClient.PostTransaction(transfer.ToAccountTransactionId, transfer.ToAccount, transfer.Amount)
            );

            await Task.WhenAll(deductTask, addTask);
        }
    }
}