using System.Linq;
using System.Threading;

namespace Cleipnir.ResilientFunctions.Domain;

public class EffectContext
{
    public EffectId? Parent { get; }
    private int ImplicitId { get; set; }

    private static readonly AsyncLocal<EffectContext> Context = new();
    
    private EffectContext(EffectId parent)
    {
        Parent = parent;
        ImplicitId = -1;
    }

    private EffectContext()
    {
        Parent = null;
        ImplicitId = -1;
    }
    
    public static EffectContext Empty => new();

    public static EffectContext CurrentContext
    {
        get
        {
            var context = Context.Value;
            if (context == null)
            {
                context = new EffectContext();
                Context.Value = context;
            }
            
            return context;    
        }
    }
    
    public static void SetParent(EffectId parentId) => Context.Value = new EffectContext(parentId);

    /// <summary>
    /// Gives the current async flow a fresh context. Invoked at the start of every flow invocation: the invocation
    /// runs on a Task that captured the scheduler's execution context (user code or a watchdog restart chain), and
    /// inheriting - and mutating - a context created there would shift the implicit effect ids across incarnations
    /// and break replay determinism.
    /// </summary>
    internal static void Reset() => Context.Value = new EffectContext();

    public int NextImplicitId() => ++ImplicitId;

    public EffectId NextEffectId() => new(
        Parent == null
            ? [++ImplicitId]
            : Parent.Value.Append(++ImplicitId).ToArray()
    );
}