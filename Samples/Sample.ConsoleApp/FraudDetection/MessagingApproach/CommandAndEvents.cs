namespace ConsoleApp.FraudDetection.MessagingApproach;

public record CommandAndEvents;
public record ApproveTransaction(Transaction Transaction) : CommandAndEvents;

public record FraudDetectorResult(string FraudDetector, Transaction Transaction, bool Approved) : CommandAndEvents;
public record TransactionApproved(Transaction Transaction) : CommandAndEvents;
public record TransactionDeclined(Transaction Transaction) : CommandAndEvents;

