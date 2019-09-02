<#
.SYNOPSIS
    Writes data only to SQL Server tables.

.DESCRIPTION
    Writes data only to SQL Server tables.

.INPUTS
    You can a pipe a PSCutomObject pipe Containing a DataReader and Connection property


.PARAMETER ServerInstance
    The server/instance string for the SQL Server to write to. 'localhost\sqlinstance'

.PARAMETER Database
    The database to write to.

.PARAMETER TableName
    The table to write to. Use a Schema Prefixed Name 'schema.table'.

.PARAMETER DataReader
    A tuple containing a Connection and DataReader property.

.PARAMETER UserName
    The SQL User property of a connection string. Toi be used for SQL Authentication.

.PARAMETER Password
    The SQL Password property of a connection string. To be used for SQL Authentication.

.PARAMETER BatchSize
    The batch commit size to be used in the Bulk Insert.

.PARAMETER QueryTimeout
    THe QueryTimeout value to be used in the SQL connection.
    
.PARAMETER ConnectionTimeout
    The ConnectionTimeout value to be used in the SQL Connection.
    
.PARAMETER KeepIdentity
    The KeepIdentity value for the Bulk Insert. Set to $False if you want to force keys.
    
.PARAMETER FireTriggers
    Set to $true if you want to write to a view or table with triggers. Default will not fire triggers on the target table/view. 
    
.OUTPUTS
None
    Produces no output

.EXAMPLE
    Get-SQLDataReader -Server 'dadsql21\sql' -Database 'diStaging' -QueryString 'SELECT 1 [Column 1]' | Write-DataReaderToSQL -ServerInstance "dadsql21\sql" -Database "diStaging" -TableName '[dbo].[TestTable]' -BatchSize 1048576 -KeepIdentity $true

.NOTES
    v1.0   - Shurvir Harrilal - Initial 
#>

function Write-DataReaderToSQL
{
    [CmdletBinding()]
    param(
        [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance,
        [Parameter(Position=1, Mandatory=$true)] [string]$Database,
        [Parameter(Position=2, Mandatory=$true)] [string]$TableName,
        [Parameter(Position=3, Mandatory=$true,ValueFromPipeline = $true,ValueFromPipelinebyPropertyname = $true) ] $DataReader,
        [Parameter(Position=4, Mandatory=$false)] [string]$Username,
        [Parameter(Position=5, Mandatory=$false)] [string]$Password,
        [Parameter(Position=6, Mandatory=$false)] [Int32]$BatchSize=50000,
        [Parameter(Position=7, Mandatory=$false)] [Int32]$QueryTimeout=0,
        [Parameter(Position=8, Mandatory=$false)] [Int32]$ConnectionTimeout=15,
        [Parameter(Position=9, Mandatory=$false)] [Int32]$KeepIdentity=$false,
		[Parameter(Position=10, Mandatory=$false)] [Int32]$FireTriggers=$false
    )

    $conn=new-object System.Data.SqlClient.SQLConnection

    if ($Username)
    { $ConnectionString = "Server={0};Database={1};User ID={2};Password={3};Trusted_Connection=False;Connect Timeout={4}" -f $ServerInstance,$Database,$Username,$Password,$ConnectionTimeout }
    else
    { $ConnectionString = "Server={0};Database={1};Integrated Security=True;Connect Timeout={2}" -f $ServerInstance,$Database,$ConnectionTimeout }

    $conn.ConnectionString = $ConnectionString

    try
    {
        $conn.Open()
		
		$CopyOptions = [System.Data.SqlClient.SqlBulkCopyOptions]::Default;
		if($KeepIdentity) {$CopyOptions += [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity;}
        if($FireTriggers) {$CopyOptions += [System.Data.SqlClient.SqlBulkCopyOptions]::FireTriggers;}
        
        [System.Data.SqlClient.SqlBulkCopy] $bulkCopy = new-object System.Data.SqlClient.SqlBulkCopy ($conn,$CopyOptions,$null)
		
        $bulkCopy.DestinationTableName = $tableName
        $bulkCopy.BatchSize = $BatchSize
        $bulkCopy.BulkCopyTimeout = $QueryTimeOut
        while($DataReader.DataReader.HasRows){
            $bulkCopy.WriteToServer($DataReader.DataReader)
            $DataReader.DataReader.NextResult() | Out-Null
        }
        $conn.Close()
        $DataReader.Connection.Close()
    }
    catch
    {
        $ex = $_.Exception
        Write-Error "$ex.Message"
        continue
    }
 }
 