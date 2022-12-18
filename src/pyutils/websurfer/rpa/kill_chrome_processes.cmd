
@echo off
rem Adapted from TAGUI end_processes.cmd
set source_dpath=%1
set port=%2

cd /d %source_dpath%

if exist "%source_dpath%\unx\gawk.exe" set "path=%source_dpath%\unx;%path%"

:repeat_kill_chrome
for /f "tokens=* usebackq" %%p in (`wmic process where "caption like '%%chrome.exe%%' and commandline like '%%tagui_user_profile_ --remote-debugging-port=%port%%%'" get processid 2^>nul ^| cut -d" " -f 1 ^| sort -nur ^| head -n 1`) do set chrome_process_id=%%p
if not "%chrome_process_id%"=="" (
    taskkill /PID %chrome_process_id% /T /F > nul 2>&1
    goto repeat_kill_chrome
)

:repeat_kill_incognito_chrome
for /f "tokens=* usebackq" %%p in (`wmic process where "caption like '%%chrome.exe%%' and commandline like '%%tagui_user_profile_ --incognito --remote-debugging-port=%port%%%'" get processid 2^>nul ^| cut -d" " -f 1 ^| sort -nur ^| head -n 1`) do set chrome_process_id=%%p
if not "%chrome_process_id%"=="" (
    taskkill /PID %chrome_process_id% /T /F > nul 2>&1
    goto repeat_kill_chrome
)
