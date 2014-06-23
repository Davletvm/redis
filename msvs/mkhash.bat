for /F "tokens=1 delims=" %%i in ('git rev-parse HEAD') do set a=%%i
set b=#define REDIS_GIT_SHA1 "
set c=%a:~0,7%
set d="
echo %b%%c%%d% > mshash.h
