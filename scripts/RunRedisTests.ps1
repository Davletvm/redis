if($args.Length -ne 2)
{
    Write-Host "Exiting...Proper Usage: .\RunRedisTests.ps1 <WorkingDir> <tclPath>"
    exit -1
}

$WorkingDir = $args[0]
$tclPath = $args[1]
$tclExePath = $tclPath + "\bin\tclsh86t.exe"

Write-Host "Changing to Working Directory :" $WorkingDir
Set-Location $WorkingDir

Write-Host "Current Contents of Current folder:"
dir 

Write-Host "Copying binaries from output folder..."
Copy-Item -Path msvs\x64\Release\* -Destination src\

$arguments = "tests\test_helper.tcl"
Write-Host "Running tests now:" $tclExePath $arguments
$process = Start-Process $tclExePath -ArgumentList $arguments -NoNewWindow -Wait -PassThru
#& "$tclPath\bin\tclsh86t.exe" "tests\test_helper.tcl"
if ($process.ExitCode -ne 0)
{
    $errorMessage = "+++++++++++++++++  Running Redistests failed. Exit code was " + $process.ExitCode + ". +++++++++++++++++"
    throw $errorMessage
}
else
{
    Write-Host "+++++++++++++++++++++++++++++++++++++++++" -ForegroundColor Green
    Write-Host "Running RedisTests succeeded"  -ForegroundColor Green
    Write-Host "+++++++++++++++++++++++++++++++++++++++++" -ForegroundColor Green
}