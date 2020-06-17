@ECHO OFF

REM OURO:  Automates routine daily tasks
REM Written by Dave Andrus on June 6, 2020
REM Copyright 2020 Agile Data Guru
REM https://github.com/AgileDataGuru/Ouro

REM Install path is %OURO_INSTALL%
REM Quorum path is %OURO_QUORUM%

REM Tally daily totals
py %OURO_INSTALL%\oura_Accountant.py

REM Get daily history
py %OURO_INSTALL%\ouro_history.py --dd --md

REM Set the archive directory
SET ad=%OURO_QUORUM%\%DATE:~10,4%%DATE:~4,2%%DATE:~7,2%

REM Make the archive folder
mkdir %ad%

REM Move the files
robocopy %OURO_QUORUM% %ad% /MOV
