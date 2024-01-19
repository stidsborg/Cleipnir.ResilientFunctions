using System.Threading.Tasks;

namespace ConsoleApp.FraudDetection.MessagingApproach;

public static class FraudDetector1
{
    public static void Start()
    {
        MessageBroker.Subscribe(message =>
        {
            if (message is ApproveTransaction command) 
                _ = Approve(command.Transaction);

            return Task.CompletedTask;
        });
    }

    private static async Task Approve(Transaction transaction)
    {
        await Task.Delay(10);
        _ = MessageBroker.Send(new FraudDetectorResult(nameof(FraudDetector2), transaction, Approved: true));
    } 
}

public class FraudDetector2 
{
    public static void Start()
    {
        MessageBroker.Subscribe(message =>
        {
            if (message is ApproveTransaction command) 
                _ = Approve(command.Transaction);

            return Task.CompletedTask;
        });
    }

    private static async Task Approve(Transaction transaction)
    {
        await Task.Delay(10);
        _ = MessageBroker.Send(new FraudDetectorResult(nameof(FraudDetector2), transaction, Approved: true));
    } 
}

public class FraudDetector3 
{
    public static void Start()
    {
        MessageBroker.Subscribe(message =>
        {
            if (message is ApproveTransaction command) 
                _ = Approve(command.Transaction);

            return Task.CompletedTask;
        });
    }

    private static async Task Approve(Transaction transaction)
    {
        await Task.Delay(10);
        _ = MessageBroker.Send(new FraudDetectorResult(nameof(FraudDetector2), transaction, Approved: true));
    } 
}