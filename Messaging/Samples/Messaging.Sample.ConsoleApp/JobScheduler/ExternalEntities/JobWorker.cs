using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.JobScheduler.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.JobScheduler.ExternalEntities;

public class JobWorker
{
    private readonly int _capacity;
    private readonly MessageQueue _messageQueue;
    private readonly HashSet<Guid> _reservations = new();
    private readonly HashSet<Guid> _executingJobs = new();
    private readonly Guid _workerId = Guid.NewGuid();

    private readonly object _sync = new();

    public JobWorker(int capacity, MessageQueue messageQueue)
    {
        _capacity = capacity;
        _messageQueue = messageQueue;
        messageQueue.Subscribers += msg =>
        {
            switch (msg)
            {
                case JobReservation jr:
                    CanTakeJob(jr);
                    return;
                case JobOrder jo:
                    TakeJob(jo);
                    return;
                case JobCancellation jc:
                    CancelJob(jc);
                    return;
            }
        };
    }

    private void CanTakeJob(JobReservation jobReservation)
    {
        object response;
        lock (_sync)
        {
            if (_capacity - _reservations.Count == 0)
                response = new JobRefused(jobReservation.JobId, _workerId);
            else
            {
                _reservations.Add(jobReservation.JobId);
                response = new JobAccepted(jobReservation.JobId, _workerId);
            }
        }
        
        _messageQueue.Send(response);
    }

    private void CancelJob(JobCancellation jobCancellation)
    {
        lock (_sync)
            _reservations.Remove(jobCancellation.JobId);
    }

    private void TakeJob(JobOrder jobOrder)
    {
        if (jobOrder.WorkerId != _workerId)
            lock (_sync)
                _reservations.Remove(jobOrder.JobId);
        else 
            ExecuteJob(jobOrder);
    }

    private void ExecuteJob(JobOrder jobOrder)
    {
        lock (_sync)
            if (_executingJobs.Contains(jobOrder.JobId)) return;
            else _executingJobs.Add(jobOrder.JobId);
        
        Task
            .Delay(3_000)
            .ContinueWith(_ =>
            {
                _messageQueue.Send(new JobCompleted(jobOrder.JobId, _workerId));
                lock (_sync)
                {
                    _executingJobs.Remove(jobOrder.JobId);
                    _reservations.Remove(jobOrder.JobId);
                }
            });
    }
}