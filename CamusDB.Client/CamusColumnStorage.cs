/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// How the server may store a large value of one <c>string</c>, <c>bytes</c> or array column. The names
/// and the behaviour follow PostgreSQL's column storage modes. A column declares it with
/// <c>STORAGE PLAIN | MAIN | EXTERNAL | EXTENDED</c> and changes it with
/// <c>ALTER TABLE t ALTER COLUMN c SET STORAGE …</c>.
///
/// <para>The strategy decides the form of future writes only. Every stored row records, per cell,
/// whether the cell is compressed and whether it is stored out of the row, and a read follows those
/// marks. So a change of strategy never changes a query result and never makes an old row unreadable.
/// <c>ALTER TABLE t REWRITE STORAGE</c> converts the rows that already exist.</para>
///
/// <para>A strategy on a column of any other type is refused by the server with <c>CADB0414</c>
/// (<c>ColumnStorageNotApplicable</c>).</para>
///
/// <para>The numeric values match the server's, so they are stable.</para>
/// </summary>
public enum CamusColumnStorage
{
    /// <summary>
    /// The server default. Compress the value when compression pays, then move it out of the row when
    /// the stored form is still at or above <c>large_value_threshold_bytes</c> (2048 by default).
    /// </summary>
    Extended = 0,

    /// <summary>
    /// Never compress, never move out of the row. Use it for a value that every query reads and that
    /// does not compress, such as an embedding that a KNN query scans.
    /// </summary>
    Plain = 1,

    /// <summary>Compress the value when compression pays, and always keep it inside the row.</summary>
    Main = 2,

    /// <summary>
    /// Never compress. Move the value out of the row when it is at or above the threshold. Use it for a
    /// large value that does not compress and that most queries skip: an image, an archive.
    /// </summary>
    External = 3,
}

/// <summary>SQL spelling of <see cref="CamusColumnStorage"/>.</summary>
public static class CamusColumnStorageExtensions
{
    /// <summary>The SQL keyword for <paramref name="storage"/>: <c>PLAIN</c>, <c>MAIN</c>, <c>EXTERNAL</c> or <c>EXTENDED</c>.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not a defined member.</exception>
    public static string ToSql(this CamusColumnStorage storage) => storage switch
    {
        CamusColumnStorage.Extended => "EXTENDED",
        CamusColumnStorage.Plain => "PLAIN",
        CamusColumnStorage.Main => "MAIN",
        CamusColumnStorage.External => "EXTERNAL",
        _ => throw new ArgumentOutOfRangeException(nameof(storage), storage, "Unknown CamusDB column storage strategy."),
    };
}
