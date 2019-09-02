<#
.SYNOPSIS
Writes data only to SQL Server tables.
.DESCRIPTION
Writes data only to SQL Server tables. However, the data source is not limited to SQL Server; any data source can be used, as long as the data can be loaded to a DataTable instance or read with a IDataReader instance.
.INPUTS
None
    You cannot pipe objects to Write-DataTable
.OUTPUTS
None
    Produces no output
.EXAMPLE
$SqlConnection = new-object System.Data.SqlClient.SqlConnection "server=tcp:$SourceServerName, $SourcePort;database=$SourceDatabaseName;Integrated Security=True;"
$SqlCmd = New-Object System.Data.SqlClient.SqlCommand;
$SqlCmd.CommandText = $SType;
$SqlCmd.Connection = $SqlConnection;
$SqlCmd.CommandTimeout = 0;
$SqlConnection.Open();
$reader = $SqlCmd.ExecuteReader();
Write-DataTable -ServerInstance "Z003\R2" -Database pubscopy -TableName authors -Data $reader
$reader.Close();
$SqlConnection.Close();

This example loads a variable reader of type DataReader from query and write the datatable to another database
.NOTES
Write-DataTable uses the SqlBulkCopy class see links for additional information on this class.
Version History
v1.0   - Chad Miller - Initial release
v1.1   - Chad Miller - Fixed error message
v1.2   - Shurvir Harrilal - Changed to use DataReader
.LINK
http://msdn.microsoft.com/en-us/library/30c3y597%28v=VS.90%29.aspx
#>

function Write-DataReader
{
    [CmdletBinding()]
    param(
        [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance,
        [Parameter(Position=1, Mandatory=$true)] [string]$Database,
        [Parameter(Position=2, Mandatory=$true)] [string]$TableName,
        [Parameter(Position=3, Mandatory=$true,ValueFromPipeline = $true,ValueFromPipelinebyPropertyname = $true) ] $Data,
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
        while($Data.DataReader.HasRows){
            $bulkCopy.WriteToServer($Data.DataReader)
            $Data.DataReader.NextResult() | Out-Null
        }
        $conn.Close()
        $Data.Connection.Close()
    }
    catch
    {
        $ex = $_.Exception
        Write-Error "$ex.Message"
        continue
    }
 }
 