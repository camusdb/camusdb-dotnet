
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Data.Common;

namespace CamusDB.Client;

/// <summary>
/// Represents a parameter to a <see cref="CamusCommand" /> and optionally its mapping to DataSet columns.
/// </summary>
public sealed class CamusParameter : DbParameter, ICloneable
{
    private ColumnType _camusDbType;

    /// <summary>
    /// Specifies the data type of a field, a property, or a Parameter object of a .NET
    /// Framework data provider.
    /// </summary>    
    public override DbType DbType
    {
        get => ToDbType(_camusDbType);
        set => _camusDbType = FromDbType(value);
    }

    public ColumnType ColumnType
    {
        get => _camusDbType;
        set => _camusDbType = value;
    }

    /// <inheritdoc />
    public override ParameterDirection Direction
    {
        get => ParameterDirection.Input;
        set
        {
            if (value != ParameterDirection.Input)
                throw new InvalidOperationException("Camus only supports input parameters.");
        }
    }

    /// <summary>
    /// Initializes a new instance of the CamusParameter class.
    /// </summary>
    public CamusParameter() { }

    /// <summary>
    /// Initializes a new instance of the CamusParameter class.
    /// </summary>
    /// <param name="parameterName">
    /// The name of the parameter. For Insert, Update and Delete commands, this name should
    /// be the name of a valid column in a Camus table. In Select commands, this name should be the name of a parameter
    /// used in the SQL Query. This value is case sensitive. Must not be null.
    /// </param>
    /// <param name="type">
    /// One of the <see cref="CamusDbType" /> values that indicates the type of the parameter.
    /// Must not be null.
    /// </param>
    /// <param name="value">An object that is the value of the CamusParameter. May be null.</param>
    /// <param name="sourceColumn">
    /// The name of the DataTable source column (SourceColumn) if this CamusParameter is
    /// used in a call to Update. May be null.
    /// </param>
    public CamusParameter(
        string parameterName,
        ColumnType type,
        object? value = null,
        string? sourceColumn = null)
    {
        ParameterName = parameterName;
        ColumnType = type;
        Value = value;
        SourceColumn = sourceColumn;
    }

    public override bool IsNullable { get; set; }

    public override string? ParameterName { get; set; }

    public override int Size { get; set; }

    public override string? SourceColumn { get; set; }

    public override bool SourceColumnNullMapping { get; set; }

    public override object? Value { get; set; }

    public override void ResetDbType()
    {
        _camusDbType = ColumnType.String;
    }

    public object Clone()
    {
        return new CamusParameter(ParameterName ?? "", ColumnType, Value, SourceColumn)
        {
            DbType = DbType,
            Direction = Direction,
            IsNullable = IsNullable,
            Size = Size,
            SourceColumnNullMapping = SourceColumnNullMapping
        };
    }

    private static ColumnType FromDbType(DbType type) => type switch
    {
        DbType.Boolean => ColumnType.Bool,
        DbType.Byte => ColumnType.Integer64,
        DbType.Decimal => ColumnType.Float64,
        DbType.Double => ColumnType.Float64,
        DbType.Guid => ColumnType.Id,
        DbType.Int16 => ColumnType.Integer64,
        DbType.Int32 => ColumnType.Integer64,
        DbType.Int64 => ColumnType.Integer64,
        DbType.Single => ColumnType.Float64,
        DbType.String => ColumnType.String,
        _ => ColumnType.String
    };

    private static DbType ToDbType(ColumnType type) => type switch
    {
        ColumnType.Bool => DbType.Boolean,
        ColumnType.Float64 => DbType.Double,
        ColumnType.Id => DbType.String,
        ColumnType.Integer64 => DbType.Int64,
        ColumnType.Null => DbType.Object,
        ColumnType.String => DbType.String,
        _ => DbType.String
    };
}
