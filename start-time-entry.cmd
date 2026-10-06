@echo off
setlocal
pushd "%~dp0time_entry_app"
call npm.cmd start
set "appExitCode=%errorlevel%"
popd
if not "%appExitCode%"=="0" pause
exit /b %appExitCode%
