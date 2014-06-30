if($args.Length -ne 2)
{
    Write-Host "Exiting...Proper Usage: .\GenerateRedisVersion.ps1 <WorkingDir> <versionTag>"
    exit -1
}

$WorkingDir = $args[0]
$versionTag = $args[1]
$redisVersion = ''

Write-Host "Changing to Working Directory :" $WorkingDir
Set-Location $WorkingDir

Write-Host "Current Contents of Current folder:"
dir 

Write-Host "Reading contents of version.h file..."
$data = Get-Content "src\version.h"
foreach ($line in $data)
{
   $tokens =  $line -split " ";
   $redisVersion = $tokens[2];
   break;
}

$redisVersion = $redisVersion.Replace("`"","")

$RedisServerResourceFile = "msvs\RedisServer.rc"
$ResourceData = Get-Content $RedisServerResourceFile
$oldFileVersion = '"FileVersion", "0.0.0.0"'
$oldProductVersion = '"ProductVersion", "0.0.0.0"' 
$newFileVersion = "`"FileVersion`", `"$($redisVersion).$($versionTag)`""
$newProductVersion = "`"ProductVersion`", `"$($redisVersion).$($versionTag)`""

$NewResourceData = $ResourceData -replace $oldFileVersion, $newFileVersion
$NewResourceData = $NewResourceData -replace $oldProductVersion, $newProductVersion
Set-Content -path $RedisServerResourceFile  -value $NewResourceData

Write-Host "Done updating RedisServer.rc file..."
Write-Host "New File Version: " + $newFileVersion
Write-Host "New Product Version: " + $newProductVersion



