@echo off
SET GCMD=git rev-parse HEAD
for /F "tokens=1 delims=" %%i in ('git rev-parse HEAD') do set a=%%i
echo | set /p=%a%>mshash.inc
copy mshash.1 /A +mshash.inc /A +mshash.2 /A mshash.h /A > nul