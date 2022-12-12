@echo off
rem Adapted from TAGUI end_processes.cmd
if exist "%~dp0unx\gawk.exe" set "path=%~dp0unx;%path%"

:repeat_kill_chrome
for /f "tokens=* usebackq" %%p in (`wmic process where "caption like '%%chrome.exe%%' and commandline like '%%tagui_user_profile_ --remote-debugging-port=9222%%'" get processid 2^>nul ^| cut -d" " -f 1 ^| sort -nur ^| head -n 1`) do set chrome_process_id=%%p
if not "%chrome_process_id%"=="" (
    taskkill /PID %chrome_process_id% /T /F > nul 2>&1
    goto repeat_kill_chrome
)