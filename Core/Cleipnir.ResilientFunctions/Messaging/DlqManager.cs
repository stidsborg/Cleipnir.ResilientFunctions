using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

/// <summary>
/// Facade over the dead letter queue used both by the framework itself and by external users inspecting,
/// removing or (eventually) redriving dead lettered messages. Obtained from
/// <see cref="FunctionsRegistry.DeadLetterQueue"/>.
/// </summary>
public class DlqManager
{
    private readonly IDlqStore _dlqStore;

    internal DlqManager(IDlqStore dlqStore) => _dlqStore = dlqStore;

    public Task Append(IReadOnlyList<StoredIdAndMessage> messages) => _dlqStore.Append(messages);

    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages() => _dlqStore.GetMessages();
    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<StoredId> storedIds) => _dlqStore.GetMessages(storedIds);

    public Task Delete(IReadOnlyList<long> positions) => _dlqStore.Delete(positions);
}
