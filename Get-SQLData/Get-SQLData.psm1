<#
.SYNOPSIS
    Execute a SQL query and pipe results

.DESCRIPTION
    The description is usually a longer, more detailed explanation of what the script or function does. Take as many lines as you need.

.PARAMETER Server
    A server from which to query the resultset

.PARAMETER Database
    A Database on the source server

.PARAMETER queryString
    A valid TSQL query to execute. No GO statements.

.EXAMPLE
    Get-SQLData -Server 'localhost\sql2016' -Database 'Playpen' -QueryString 'SELECT TOP 100 * FROM  [PlayPen].[dbo].[TestImportCSV]' | Out-GridView

.Outputs
    Outputs the rows as PSCustomObjects to the Pipeline
#>

function Get-SQLData
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
        $schemaTable = $DataReader.GetSchemaTable();

        $holdingObject = [pscustomobject]@{}
        
        # Create Holding Object From DataReader Schema
        for($i = 0; $i -lt $schemaTable.Rows.Count; $i++){
            $holdingObject | Add-Member -MemberType NoteProperty -Name $($schemaTable.Rows[$i].ColumnName.ToString()) -Value $null
        }

        # Iterate through the data
        do {
            while ($DataReader.HasRows)
            {
                while($DataReader.Read()){
                    for($i = 0; $i -lt $schemaTable.Rows.Count; $i++){
                        $holdingObject."$($schemaTable.Rows[$i].ColumnName)" = $DataReader.GetValue($i)
                    }              
                    $holdingObject
                }
                $hasNextRS = $DataReader.NextResult()
            }
        }
        while($DataReader.NextResult())
        $connection.Close()

    }
    catch
    {
        $_.Exception.Message
        $_.Exception.ItemName
        $connection.Close();
    }
};

