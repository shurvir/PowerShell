<#
.SYNOPSIS

Execute a SQL query and return a resultset/s
.DESCRIPTION

The description is usually a longer, more detailed explanation of what the script or function does. Take as many lines as you need.
.PARAMETER connectionString

An ADO.NET connection string for a SQL Server Instance
.PARAMETER queryString

A valid TSQL query to execute. No GO statements.
.EXAMPLE

Get-ODBC-Results -connectionString "Data Source=dadsql21\sql;database=diStaging;Integrated Security=SSPI;" -queryString "SELECT GETDATE() [Date]"

.OUTPUTS
    Outputs a DataSet Object containing result sets returned by the query.

#>

function Get-ODBCResults
(
    $connectionString,
    $queryString
)
{
    $connection = new-object System.Data.Odbc.OdbcConnection;
    $connection.ConnectionString = $connectionString;
    $connection.Open();

    $command = $connection.CreateCommand();
    $command.Connection = $connection;
	$command.CommandText = $queryString;
	$command.CommandTimeout = 0

    $SqlAdapter = New-Object System.Data.Odbc.OdbcDataAdapter;
	$SqlAdapter.SelectCommand = $command;
	$DataSet = New-Object System.Data.DataSet;


    try
    {
	    $SqlAdapter.Fill($DataSet)
        $connection.Close();
	    return $DataSet;

    }
    catch
    {
        $_.Exception.Message
        $_.Exception.ItemName
    }

    $connection.Close();
}