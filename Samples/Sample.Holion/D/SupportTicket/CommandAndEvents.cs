namespace Sample.Holion.D.SupportTicket;

public record CommandAndEvents;
public record TakeSupportTicket(Guid Id, string CustomerSupportAgentEmail, string RequestId) : CommandAndEvents;
public record SupportTicketTaken(Guid Id, string CustomerSupportAgentEmail, string RequestId) : CommandAndEvents;

