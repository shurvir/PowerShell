<#
.SYNOPSIS
Writes Powershell Object data only to SQL Server tables.
.DESCRIPTION
Writes data only to SQL Server tables. However, the data source is not limited to SQL Server; any data source can be used, as long as the data can be loaded to a DataTable instance or read with a IDataReader instance.
.INPUTS
None
    You cannot pipe objects to Create-PSObjectInSQL
.OUTPUTS
None
    Produces no output
.EXAMPLE
$Data = GCI -File | Select FullName
Write-PSObject -ServerInstance "localhost\sql2016" -Database "PlayPen" -TableName "GCIQueryRes" -SchemaName "dbo" -Data $Data
This example loads a variable Data of type PSObject [] from a Powershell Query and write the datatable to another database
.NOTES
Write-PSObject uses the SqlBulkCopy class
v1.0   - Shurvir Harrilal - Initial release
#>
function Create-PSObjectInSQL
{
    [CmdletBinding()]
    param(
    [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance,
    [Parameter(Position=1, Mandatory=$true)] [string]$Database,
    [Parameter(Position=2, Mandatory=$true)] [string]$TableName,
    [Parameter(Position=3, Mandatory=$true)] [string]$SchemaName,
    [Parameter(Position=4, Mandatory=$true)] $Data,
    [Parameter(Position=5, Mandatory=$false)] [string]$Username,
    [Parameter(Position=6, Mandatory=$false)] [string]$Password,
    [Parameter(Position=7, Mandatory=$false)] [Int32]$BatchSize=50000,
    [Parameter(Position=8, Mandatory=$false)] [Int32]$QueryTimeout=0,
    [Parameter(Position=9, Mandatory=$false)] [Int32]$ConnectionTimeout=15,
    [Parameter(Position=10, Mandatory=$false)] [Int32]$KeepIdentity=$false,
	[Parameter(Position=11, Mandatory=$false)] [Int32]$FireTriggers=$false
    )

    begin{
        
        Add-Type -path "C:\Windows\assembly\GAC_MSIL\Microsoft.SqlServer.Smo\13.0.0.0__89845dcd8080cc91\Microsoft.SqlServer.Smo.dll"

	    $SourceUseWinAuth = $TRUE
	    $DestinationUseWinAuth = $TRUE
        $IdentityInsert = $TRUE
        $ErrorRows = @();

        function build-Table
        ([System.Object[]] $table, $tableName, $schemaName)
        {
            # Create and open a database connection
	        $SqlConnection = new-object System.Data.SqlClient.SqlConnection "server=$ServerInstance;database=$Database;Integrated Security=True;"

            [Microsoft.SqlServer.Management.Smo.Server] $objServer = new-object Microsoft.SqlServer.Management.Smo.Server $SqlConnection.DataSource;
            [Microsoft.SqlServer.Management.Smo.Database] $objDB = $objServer.Databases[$SqlConnection.Database];

            $T = new-object Microsoft.SqlServer.Management.Smo.Table ($objDB, $tableName, $schemaName);

            $C = new-object Microsoft.SqlServer.Management.Smo.Column; 

            $table | gm -MemberType Properties | % {
                $dc = $_;
                $C = new-object Microsoft.SqlServer.Management.Smo.Column($T, $dc.name);
                $C.DataType =  New-Object Microsoft.SqlServer.Management.Smo.DataType([Microsoft.SqlServer.Management.Smo.SqlDataType]::VarCharMax);
                $T.Columns.Add($C);
            }

            $T.Create();
        }

        function insert-Data
        {
            param([System.String] $TName, [System.String] $SName, [System.Data.DataTable] $DTable)

            # Create and open a database connection
	        $SqlConnection = new-object System.Data.SqlClient.SqlConnection "Server=$ServerInstance;Database=$Database;Integrated Security=True;";
	
            $SqlConnection.Open();
			
			$CopyOptions = [System.Data.SqlClient.SqlBulkCopyOptions]::Default;
			if($KeepIdentity) {$CopyOptions += [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity;}
			if($FireTriggers) {$CopyOptions += [System.Data.SqlClient.SqlBulkCopyOptions]::FireTriggers;}
        
			[System.Data.SqlClient.SqlBulkCopy] $bulkCopy = new-object System.Data.SqlClient.SqlBulkCopy ($SqlConnection,$CopyOptions,$null)

            $bulkCopy.DestinationTableName = "[$SName].[$TName]";
            $bulkCopy.BulkCopyTimeout = 0;
            $bulkCopy.BatchSize = $BatchSize;
        
            try
            {
	            ## Write from the source to the destination.
	            $bulkCopy.WriteToServer($DTable);
            }
            catch
            {
	            $_.Exception.Message | Out-Host
                $Global:ErrorRows = $DTable.Rows
	            $bulkCopy = $null
                exit
            }
            $bulkCopy = $null
	 
	
            $SqlConnection.Close()
        }

        function create-Dataset
        ([System.Object[]] $table)
        {
            [System.Data.DataTable] $returnDataTable = New-Object System.Data.DataTable;

            $properties = ($table | gm -MemberType Properties);

            $properties | % {
	            [void] $returnDataTable.Columns.Add($_.name,[string]);
            }
        
            for($i=0; $i -lt $table.Length; $i++)
            {
                $row = $table[$i];
                $dataRow = $returnDataTable.NewRow();

                $properties | % {
	                $dataRow[$_.name] = $row."$($_.name)";
                }
            
                [void] $returnDataTable.Rows.Add($dataRow);
	        }

            [void] $returnDataTable.AcceptChanges();

            return ,$returnDataTable;
        }

    }
    
    Process{
        <# Check Table Exists#>
        if((Invoke-Sqlcmd -ServerInstance "$ServerInstance" -Database "$Database" -Query "SELECT OBJECT_ID('[$SchemaName].[$TableName]') [Result]").Result -eq [System.DBNull]::Value){
            build-Table -table $Data -tableName $TableName -schemaName $SchemaName
        }
        
        <#  Process Data in Batches #>
        for($i=0; $i -lt $Data.Length; $i = $i + $batchSize){
            if($i + $batchSize -lt $Data.Length){
                $end = $i + $batchSize - 1;
            } else { 
                $end = $Data.Length - 1;
            }

            $batchData = $Data[$i..$end];
            
            $dataTable =  [System.Data.DataTable] (create-Dataset -table $batchData);

            insert-Data -TName $tableName -SName $SchemaName -DTable $dataTable;
        }
    }
}
