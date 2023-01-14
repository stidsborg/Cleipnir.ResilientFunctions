using System;

namespace ConsoleApp.LoanApproval;

public record LoanApplication(string Id, Guid CustomerId, decimal Amount, DateTime Created);