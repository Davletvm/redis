if($args.Length -ne 6)
{
    Write-Host "Exiting...Proper Usage: .\CopyBinaries.ps1 <ProjectName> <WorkingDir> <Branch> <BuildNumber> <BinCopyfolder> <DropPath>"
    exit -1
}

$ProjName=$args[0]
$WorkingDir=$args[1]
$Branch=$args[2]
$BuildNumber=$args[3]
$BinCopyFromFolder=$args[4]
$DropPath=$args[5]

Write-Host "Changing to Working Directory: " $WorkingDir
Set-Location $WorkingDir

Write-Host "Current Contents of Current folder:"
dir 

#Get Todays Date
$CurrentDate = Get-Date
$CurrentDateStr = $CurrentDate.ToString("yyyyMMdd")
Write-Host "date is :" $CurrentDateStr
$BuildDropFolder="$DropPath\$ProjName\$Branch\" + "$CurrentDateStr" + "_" + $BuildNumber

Write-Host "Creating Drop folder for this build.."
New-Item -ItemType directory "$BuildDropFolder\Binaries"
New-Item -ItemType directory "$BuildDropFolder\Src"

Write-Host "Copying binaries from output folder '$BinCopyFromFolder' to '$BuildDropFolder\Binaries'"
Copy-Item -Path $BinCopyFromFolder\* -Destination $BuildDropFolder\Binaries

Write-Host  "Copying sources to '$BinCopyFromFolder' to '$BuildDropFolder\Src'"
Get-ChildItem -path "src\*" -recurse -include "*.c","*.cpp",".h" |
  Foreach-Object { Copy-Item -path $_ -destination "$BuildDropFolder\Src"}

$latestDropfolder = "$DropPath\$ProjName\$Branch" + "\Latest" 
Write-Host "Updating the Latest HardLink..."
If (Test-Path $latestDropfolder){
	Remove-Item -Recurse -Force $latestDropfolder
}

cmd /c mklink /D /J $latestDropfolder $BuildDropFolder