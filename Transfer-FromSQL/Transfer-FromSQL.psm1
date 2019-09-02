<#
.SYNOPSIS
    Execute a SQL query and pass the results to a destination.

.DESCRIPTION
    A Coommand Pattern POC type of cmdlet, done to better manage the transaction of the source connection.
    It executes a Query on a SQL Server and Writes the Reult to a Destination. The cmdlet takes a ScriptBlock as a parameter.
    This is used as the Writer. This ScriptBlock will action the results of the SQL Query.

.PARAMETER Server
    A server from which to query the resultset

.PARAMETER Database
    A Database on the source server

.PARAMETER QueryString
    A valid TSQL query to execute. No GO statements.

.PARAMETER WriteBlock
    A PowerShell ScriptBlock that manipulates a DataReader object. The ScriptBlock needs to accept a [System.Data.SqlClient.SqlDataReader] parameter named Data.

.EXAMPLE
    Transfer-FromSQL -Server 'dadsql21\sql' -Database 'diStaging' -QueryString 'SELECT 1 [Column 1]' -WriteBlock {param([System.Data.SqlClient.SqlDataReader]$Data) Write-DataReader -ServerInstance "dadsql21\sql" -Database "diStaging" -TableName '[dbo].[TestTable]' -Data $Data -BatchSize 1048576 -KeepIdentity $true}

.EXAMPLE
    Transfer-FromSQL -Server 'dadsql21\sql' -Database 'diStaging' -QueryString 'SELECT 1 [Column 1]' -WriteBlock {param([System.Data.SqlClient.SqlDataReader]$Data) Write-DataReaderToCSV -Reader $Data -Path "C:\Projects\Working\testcsv.txt" -IncludeHeader}

.Outputs
    None
#>

function Transfer-FromSQL
(
    $Server,
	$Database,
    $QueryString,
    [ScriptBlock] $WriteBlock
)
{
    if($WriteBlock.Ast.ParamBlock.Parameters | where {$_.StaticType -eq [System.Data.SqlClient.SqlDataReader] -and $_.Name.ToString() -eq '$data'}){

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
            & $WriteBlock -Data $DataReader
            $connection.Close();

        }
        catch
        {
            $_.Exception.Message
            $_.Exception.ItemName
            $connection.Close();
        }
    } else{

        Write-Error 'WriteBlock Error: There is no [System.Data.SqlClient.SqlDataReader] Data Parameter in the ScriptBlock'
    }
};