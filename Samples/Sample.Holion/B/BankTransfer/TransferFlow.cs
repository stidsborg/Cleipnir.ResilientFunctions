namespace Sample.Holion.B.BankTransfer;

public sealed class TransferFlow : Flow<Transfer>
{
    private IAccountService AccountService { get; }
    
    public TransferFlow(IAccountService accountService) => AccountService = accountService;
    
    public override async Task Run(Transfer transfer)
    {
        await Task.CompletedTask;
        throw new NotImplementedException();
    }
}