<#
.SYNOPSIS
    Execute a SQL query and return a resultset/s

.DESCRIPTION
    Executes a SQL Query and Returns a DataSet object encapsulating the results from the SQL Query..

.PARAMETER Server
    A server from which to query the resultset

.PARAMETER Database
    A Database on the source server

.PARAMETER queryString
    A valid TSQL query to execute. No GO statements.

.EXAMPLE
    Get-SQL-Results -ConnectionString "Data Source=dadsql21\sql;database=diStaging;Integrated Security=SSPI;" -QueryString "SELECT GETDATE() [Date]"

.Outputs
    A DataSet object containing the results from the query.
#>

function Get-SQLResults
(
    $Server,
	$Database,
    $QueryString
)
{
    [System.Data.SqlClient.SqlConnection] $connection = new-object System.Data.SqlClient.SqlConnection;
    $connectionStringBuilder = new-object System.Data.SqlClient.SqlConnectionStringBuilder;

    $connectionStringBuilder["Data Source"] = $Server
	$connectionStringBuilder["Integrated Security"] = $true
	$connectionStringBuilder["Initial Catalog"] = $Database

    $connection.ConnectionString = $connectionStringBuilder.ConnectionString;
    $connection.Open();

    $command = $connection.CreateCommand();
    $command.Connection = $connection;
	$command.CommandText = $queryString;
	$command.CommandTimeout = 0

    $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter;
	$SqlAdapter.SelectCommand = $command;
	$DataSet = New-Object System.Data.DataSet;


    try
    {
	    [void] $SqlAdapter.Fill($DataSet)
            $connection.Close();
	    return $DataSet;

    }
    catch
    {
        $_.Exception.Message
        $_.Exception.ItemName
    }

    $connection.Close();
};
