<#
.SYNOPSIS

Execute a SQL query and return a datareader, connection pair
.DESCRIPTION

The description is usually a longer, more detailed explanation of what the script or function does. Take as many lines as you need.
.PARAMETER connectionString

An ADO.NET connection string for a SQL Server Instance
.PARAMETER queryString

A valid TSQL query to execute. No GO statements.
.EXAMPLE

Get-ODBC-Results -connectionString "Data Source=dadsql21\sql;database=diStaging;Integrated Security=SSPI;" -queryString "SELECT GETDATE() [Date]"
#>

function Get-ODBCDataReader
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

    try
    {
        $DataReader = $command.ExecuteReader();
	    return @{Connection=$connection;DataReader=$DataReader};
    }
    catch
    {
        $_.Exception.Message
        $_.Exception.ItemName
        $connection.Close();
    }
}
