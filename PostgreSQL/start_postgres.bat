REM Only run the initdb command once, then comment the line - place REM in front
@ECHO ON
@SET "PATH=%~dp0bin;%PATH%"
@SET PGDATA=%~dp0data
@SET PGDATABASE=postgres
@SET PGUSER=postgres
@SET PGPORT=5432
@SET PGLOCALEDIR=%~dp0share\locale
REM initdb -U postgres -W -E UTF8 -D "%PGDATA%" --auth=trust
"%~dp0bin\pg_ctl" -D "%PGDATA%" -o "-p %PGPORT%" -l logfile start
pause