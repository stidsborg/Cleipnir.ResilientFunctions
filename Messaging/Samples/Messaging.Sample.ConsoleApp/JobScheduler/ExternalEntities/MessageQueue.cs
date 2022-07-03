namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.JobScheduler.ExternalEntities;

public class MessageQueue
{
    private readonly List<Func<object, Task>> _subscribers = new();
    private readonly object _sync = new();

    public void Subscribe(Func<object, Task> handler)
    {
        lock (_sync)
            _subscribers.Add(handler);
    }
    
    public void Send(object msg)
    {
        Console.WriteLine("MESSAGE_QUEUE SENDING: " + msg.GetType());
        Task.Run(() =>
        {
            List<Func<object, Task>> subscribers;
            lock (_sync)
                subscribers = _subscribers.ToList();

            foreach (var subscriber in subscribers)
                _ = subscriber(msg);
        });
    }
}