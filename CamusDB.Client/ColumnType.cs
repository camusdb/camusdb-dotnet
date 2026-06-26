
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

// IMPORTANT: These integer values are the wire contract with the CamusDB server
// (CamusDB.Core.Catalogs.Models.ColumnType). They are persisted in schema JSON and
// must never be renumbered or reused. New members are appended with new integers.
public enum ColumnType
{
    Null = 0,
    Id = 1,
    Integer64 = 2,
    String = 3,
    Bool = 4,
    Float64 = 5,
    Float32 = 6,
    Bytes = 7,
    Date = 8,
    DateTime = 9,
    Array = 10,
}
