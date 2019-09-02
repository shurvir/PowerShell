<#
.SYNOPSIS
    Execute a SQL query and return a SQL DataReader.

.DESCRIPTION
    Executes a query on a SQL server and returns a Connection and DataReader Object. These can be used to consume data coming from the connection.
    The receiver of this object will need to iterate through the results of the DataReader and Close the Connection once done.

.PARAMETER Server
    A SQL server from which to query

.PARAMETER Database
    A Database on the source server

.PARAMETER queryString
    A valid TSQL query to execute. No GO statements.

.EXAMPLE
    Get-SQL-Results -ConnectionString "Data Source=dadsql21\sql;database=diStaging;Integrated Security=SSPI;" -QueryString "SELECT GETDATE() [Date]"

.Outputs
    Returns a Wrapped DataReader and SqlConnection object.
#>

function Get-SQLDataReader
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
};