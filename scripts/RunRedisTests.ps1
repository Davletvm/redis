if($args.Length -ne 2)
{
    Write-Host "Exiting...Proper Usage: .\RunRedisTests.ps1 <WorkingDir> <tclPath>"
    exit -1
}

$WorkingDir = $args[0]
$tclPath = $args[1]

Write-Host "Changing to Working Directory :" $WorkingDir
Set-Location $WorkingDir

Write-Host "Current Contents of Current folder:"
dir 

Write-Host "Copying binaries from output folder..."
Copy-Item -Path msvs\x64\Release\* -Destination src\

Write-Host "Running tests now:"
& "$tclPath\bin\tclsh86t.exe" "tests\test_helper.tcl"