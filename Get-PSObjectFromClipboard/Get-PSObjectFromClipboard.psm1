<#
.SYNOPSIS
Puts a pscustomobject definition of the current clipboard contents into the clipboard.
.DESCRIPTION
Puts a pscustomobject definition of the current clipboard contents into the clipboard.
.INPUTS
None
    You can pipe objects to Write-PSObject
.OUTPUTS
None
    Produces no output
.EXAMPLE
Get-PSObjectFromClipboard
v1.0   - Shurvir Harrilal - Initial release
#>
Function Get-PSObjectFromClipboard {
    $clipper = get-clipboard
    $header = ''
    $text = "@("
    $clipper | %{
        if($header.Length -eq 0 -and $_.ToString().Trim().Length -ne 0){
            $header = $_.ToString()
            $headercols = $header.Split("`t")
        }
        elseif($_.ToString().Trim().Length -gt 0){
            $text += "`r`n[pscustomobject] @{"
            $datacols = $_.ToString().Split("`t")
            for($i = 0; $i -lt $datacols.Length; $i++){
                $text += "$($headercols[$i]) = `"$($datacols[$i].Replace('"','``"'))`"$(if($i -eq $datacols.Length -1){''}else{';'})"
            }
            $text += "`r`n},"
        }
    }
    $text = $text.ToString().TrimEnd(',') + ')'
    $text | clip
}

