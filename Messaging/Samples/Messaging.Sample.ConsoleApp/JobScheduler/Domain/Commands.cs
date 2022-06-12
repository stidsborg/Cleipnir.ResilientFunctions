namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.JobScheduler.Domain;

public record JobReservation(Guid JobId);
public record JobOrder(Guid JobId, Guid WorkerId);
public record JobCancellation(Guid JobId);