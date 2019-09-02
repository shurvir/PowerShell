<#
.SYNOPSIS
    Writes Powershell Object data only to SQL Server tables.

.DESCRIPTION
    Writes data only to SQL Server tables. The performance on the cmdlet is slow compared with traditional bulk loading methods.
    Try to keep input under 1 million items.

.INPUTS
Data
    You can pipe objects to Write-PSObjectToSQL

.OUTPUTS
None
    Produces no output

.EXAMPLE
    GCI -File | Select FullName | Write-PSObject -ServerInstance "localhost\sql2016" -Database "PlayPen" -TableName "GCIQueryRes" -SchemaName "dbo"
    This example loads a variable Data of type PSObject [] from a Powershell Query and writes the datatable to another database

.NOTES
    Write-PSObject uses the SqlBulkCopy class
    v1.0   - Shurvir Harrilal - Initial release
#>
function Write-PSObjectToSQL
{
    [CmdletBinding()]
    param(
    [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance,
    [Parameter(Position=1, Mandatory=$true)] [string]$Database,
    [Parameter(Position=2, Mandatory=$true)] [string]$TableName,
    [Parameter(Position=3, Mandatory=$true)] [string]$SchemaName = 'dbo',
    [Parameter(Position=4, Mandatory=$true,ValueFromPipeline = $true)] $Data,
    [Parameter(Position=5, Mandatory=$false)] [string]$Username,
    [Parameter(Position=6, Mandatory=$false)] [string]$Password,
    [Parameter(Position=7, Mandatory=$false)] [Int32]$BatchSize=50000,
    [Parameter(Position=8, Mandatory=$false)] [Int32]$QueryTimeout=0,
    [Parameter(Position=9, Mandatory=$false)] [Int32]$ConnectionTimeout=15,
    [Parameter(Position=10, Mandatory=$false)] [Int32]$KeepIdentity=$false,
	[Parameter(Position=11, Mandatory=$false)] [Int32]$FireTriggers=$false
    )

    begin{
	    $SourceUseWinAuth = $TRUE
	    $DestinationUseWinAuth = $TRUE
        $IdentityInsert = $TRUE
        $ErrorRows = @();
        $Batch = @();

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
        $Batch += $Data;

        if($Batch.Length -ge $BatchSize){
            $dataTable =  [System.Data.DataTable] (create-Dataset -table $Batch)
            insert-Data -TName $tableName -SName $SchemaName -DTable $dataTable
            $Batch = @()
        }
    }

    end{
        if($Batch.Length -gt 0){
            $dataTable =  [System.Data.DataTable] (create-Dataset -table $Batch)
            insert-Data -TName $tableName -SName $SchemaName -DTable $dataTable
            $Batch = @()
        }
    }
}
