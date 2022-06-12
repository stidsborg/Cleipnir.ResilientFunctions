namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.JobScheduler.Domain;

public record JobAccepted(Guid JobId, Guid WorkerId);
public record JobRefused(Guid JobId, Guid WorkerId);
public record JobCompleted(Guid JobId, Guid WorkerId);