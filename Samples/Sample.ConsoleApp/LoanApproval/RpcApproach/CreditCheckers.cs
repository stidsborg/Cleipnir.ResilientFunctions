using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace ConsoleApp.LoanApproval.RpcApproach;

public interface ICreditChecker
{
    Task<bool> Approve(LoanApplication loanApplication);
}

public class CreditChecker1 : ICreditChecker
{
    public Task<bool> Approve(LoanApplication loanApplication) => true.ToTask();
}

public class CreditChecker2 : ICreditChecker
{
    public Task<bool> Approve(LoanApplication loanApplication) => true.ToTask();
}

public class CreditChecker3 : ICreditChecker
{
    public Task<bool> Approve(LoanApplication loanApplication) => true.ToTask();
}