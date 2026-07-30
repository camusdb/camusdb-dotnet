
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Transport;

/// <summary>
/// The CamusDB error codes a prepared execution has to reason about, rather than match on by literal.
/// </summary>
internal static class CamusPreparedStatementErrorCodes
{
    /// <summary>
    /// The node does not recognize the handle — closed, expired, minted before a restart, minted on
    /// another node, or minted on a stream that has since been rebuilt. Routine by contract, not a bug:
    /// the transport prepares again and replays the execution once.
    /// </summary>
    public const string UnknownPreparedStatement = "CADB0520";

    /// <summary>The registration was refused because a server-side cap is full. The statement runs inline
    /// instead; nothing about it is retried, since a second PREPARE would meet the same cap.</summary>
    public const string LimitExceeded = "CADB0521";
}

/// <summary>
/// Turns this driver's named parameters into the ordinal values a prepared execution sends.
///
/// <para>Prepared executions are positional by design — dropping the names is much of why they are
/// cheaper — but the ADO surface binds by name and must keep doing so. The published
/// <c>parameterNames</c> are therefore the pivot: the server tells the client the binding order once, at
/// registration, and the client maps its own dictionary onto that order before every write. Both
/// transports share this type so a statement binds identically over REST and gRPC.</para>
/// </summary>
internal static class PreparedStatementBinder
{
    /// <summary>
    /// Orders <paramref name="parameters"/> by <paramref name="parameterNames"/>, converting each value
    /// through <paramref name="convert"/> so a transport materializes only its own wire type.
    ///
    /// <para>A declared placeholder with no matching parameter throws: a prepared execution sends values
    /// by position and the count must match exactly, so there is no value this layer could legitimately
    /// invent. Callers treat that throw as "run this one inline" rather than as the statement's verdict,
    /// which is what keeps an unbound parameter reported the way inline execution reports it — by the
    /// engine, if and when it actually evaluates the placeholder.</para>
    ///
    /// <para>Extra parameters the statement does not reference are simply not sent. Inline execution
    /// ignores them too, and refusing them here would make preparing change the meaning of a command that
    /// already worked.</para>
    /// </summary>
    public static List<TValue> Bind<TValue>(
        IReadOnlyList<string> parameterNames,
        IReadOnlyDictionary<string, ColumnValue>? parameters,
        Func<ColumnValue, TValue> convert)
    {
        List<TValue> values = new(parameterNames.Count);

        foreach (string name in parameterNames)
        {
            if (!TryLookup(parameters, name, out ColumnValue value))
                throw new CamusException(
                    "CADB0400",
                    $"Prepared statement declares parameter '{name}' but no value was bound for it");

            values.Add(convert(value));
        }

        return values;
    }

    /// <summary>
    /// Finds the value for a published placeholder name.
    ///
    /// <para>Matching is exact, and deliberately so. The server publishes names verbatim, including the
    /// leading <c>@</c>, and that is also the name an inline execution binds by — an inline statement
    /// whose parameter is added as <c>year</c> for a <c>@year</c> placeholder is rejected. Accepting the
    /// looser spelling here would make preparing a command change which spellings work, so a statement
    /// would run one way before it crossed the auto-prepare threshold and another way after.</para>
    /// </summary>
    private static bool TryLookup(
        IReadOnlyDictionary<string, ColumnValue>? parameters, string name, out ColumnValue value)
    {
        value = default!;

        return parameters is not null && parameters.TryGetValue(name, out value!);
    }
}
