using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace ConsoleApp.FraudDetection.RpcApproach;

public interface IFraudDetector
{
    Task<bool> Approve(Transaction transaction);
}

public class FraudDetector1 : IFraudDetector
{
    public Task<bool> Approve(Transaction transaction) => true.ToTask();
}

public class FraudDetector2 : IFraudDetector
{
    public Task<bool> Approve(Transaction transaction) => true.ToTask();
}

public class FraudDetector3 : IFraudDetector
{
    public Task<bool> Approve(Transaction transaction) => true.ToTask();
}