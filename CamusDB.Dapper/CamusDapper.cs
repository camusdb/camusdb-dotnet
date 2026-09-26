/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Dapper;

namespace CamusDB.Dapper;

/// <summary>
/// Registers the CamusDB type handlers with Dapper.
///
/// <para>Dapper works with a <see cref="CamusConnection"/> without any of this: it is a plain ADO.NET
/// connection. The handlers add the CamusDB types Dapper does not know. Dapper keeps type handlers in
/// one process-wide table, so call these methods once at startup.</para>
/// </summary>
public static class CamusDapper
{
    private static int registered;

    /// <summary>
    /// Registers the handlers that change no existing Dapper behavior: <see cref="CamusObjectIdTypeHandler"/>
    /// for <see cref="CamusDB.Core.Util.ObjectIds.CamusObjectIdValue"/>. Calling it more than once has no effect.
    /// </summary>
    public static void Register()
    {
        if (Interlocked.Exchange(ref registered, 1) == 1)
            return;

        SqlMapper.AddTypeHandler(new CamusObjectIdTypeHandler());
    }

    /// <summary>
    /// Registers a native-array handler for the common element types: <see cref="long"/>,
    /// <see cref="int"/>, <see cref="double"/>, <see cref="bool"/>, <see cref="string"/> and
    /// <see cref="Guid"/>.
    ///
    /// <para>This is opt-in because it changes what Dapper does with an array parameter of those types:
    /// Dapper binds it as one <c>ARRAY(T)</c> value instead of expanding <c>IN @values</c> into a list.
    /// Write <c>= ANY(@values)</c> for a membership test after you register the handlers. Without them,
    /// wrap a value in <see cref="CamusValue.Array{T}(IEnumerable{T})"/> to bind it as an array.</para>
    /// </summary>
    public static void RegisterArrayTypeHandlers()
    {
        RegisterArrayTypeHandler<long>();
        RegisterArrayTypeHandler<int>();
        RegisterArrayTypeHandler<double>();
        RegisterArrayTypeHandler<bool>();
        RegisterArrayTypeHandler<string>();
        RegisterArrayTypeHandler<Guid>();
    }

    /// <summary>
    /// Registers a native-array handler for <typeparamref name="T"/>[], with the element type inferred
    /// from <typeparamref name="T"/>.
    /// </summary>
    public static void RegisterArrayTypeHandler<T>()
        => SqlMapper.AddTypeHandler(new CamusArrayTypeHandler<T>());

    /// <summary>
    /// Registers a native-array handler for <typeparamref name="T"/>[] with an explicit element type.
    /// </summary>
    public static void RegisterArrayTypeHandler<T>(ColumnType elementType)
        => SqlMapper.AddTypeHandler(new CamusArrayTypeHandler<T>(elementType));
}
