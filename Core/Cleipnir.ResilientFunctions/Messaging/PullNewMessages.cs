using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Messaging;

public delegate Task<IReadOnlyList<StoredMessage>> PullNewMessages();