using System;

namespace ConsoleApp.SupportTicket;

public record SupportTicketRequest(Guid SupportTicketId, string[] CustomerSupportAgents);