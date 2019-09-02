<#
.SYNOPSIS
    Read a Dotnet DataReader and write it to a CSV File

.DESCRIPTION
    Writes data only to SQL Server tables. However, the data source is not limited to SQL Server; any data source can be used, as long as the data can be loaded to a DataTable instance or read with a IDataReader instance.

.INPUTS
    None

.PARAMETER Path
    The outpur path for the CSV file

.PARAMETER Reader
    The DataReader object to be used to retrieve data.

.PARAMETER IncludeHeader
    A switch to determine whether the header line is included or not.

.OUTPUTS
None
    Produces no output

.EXAMPLE
 $SQLReader = Get-SQL-DataReader -ConnectionString "Data Source=dadsql21\sql;database=distaging;Integrated Security=SSPI;" -QueryString "SELECT TOP 100 ID,MPL_Number,UMID,Date_Responded,Caller_UMID,Strategy_Code,List_Owner_Code,InsertDate FROM [extract].[MPL_Person_Campaigns] with (nolock)"
 Write-DataReaderToCSV -Reader $SQLReader -Path "C:\Projects\Working\xpcsv.txt" -IncludeHeader

.EXAMPLE
 Get-SQL-DataReader -ConnectionString "Data Source=dadsql21\sql;database=distaging;Integrated Security=SSPI;" -QueryString "SELECT TOP 100 ID,MPL_Number,UMID,Date_Responded,Caller_UMID,Strategy_Code,List_Owner_Code,InsertDate FROM [extract].[MPL_Person_Campaigns] with (nolock)" |
 Write-DataReaderToCSV -Path "C:\Projects\Working\xpcsv.txt" -IncludeHeader

.NOTES
    v1.0   - Shurvir Harrilal - Initial release
    Need to Parametize the Text Encoding.
#>
function Write-DataReaderToCSV
{
    [CmdletBinding()]
    param(
        [Parameter(Position=0, Mandatory=$true)] [string]$Path,
        [Parameter(Position=1, Mandatory=$true,ValueFromPipeline = $true,ValueFromPipelinebyPropertyname = $true) ] $DataReader,
        [Parameter(Position=2, Mandatory=$false)] [switch] $IncludeHeader
    )

    try
    {
        $sw = New-Object System.IO.StreamWriter($Path, $false, [System.Text.UTF8Encoding]::UTF8)

        $schemaTable = $DataReader.DataReader.GetSchemaTable()
        if($IncludeHeader){
            $line = ''
            for($i = 0; $i -lt $schemaTable.Rows.Count; $i++){
                $line += '"' + $schemaTable.Rows[$i].ColumnName.ToString().Replace('"','""') + '",'
            }
            $sw.Writeline($line.Substring(0,$line.Length -1))
        }

        do {
            while ($DataReader.DataReader.HasRows)
            {

                while($DataReader.DataReader.Read()){
                    $line = ''
                    for($i = 0; $i -lt $schemaTable.Rows.Count; $i++){
                        if($DataReader.DataReader.GetValue($i) -is [String]){
                            $line += '"' + $DataReader.DataReader.GetValue($i).ToString().Replace('"','""') + '",'
                        }else {
                            $line += '' + $DataReader.DataReader.GetValue($i).ToString().Replace('"','""') + ','
                        }
                    }
                    
                    $sw.Writeline($line.Substring(0,$line.Length -1))
                }

                $hasNextRS = $DataReader.DataReader.NextResult()
            }
        }
        while($DataReader.DataReader.NextResult())

        $sw.Close()
        $DataReader.Connection.Close()
    }
    catch
    {
        $ex = $_.Exception
        Write-Error "$ex.Message"
        continue
    }
    finally
    {
        if ($null -ne $sw -and $sw -is [System.IDisposable])
        {
            $sw.Dispose()
        }
    }
 }

