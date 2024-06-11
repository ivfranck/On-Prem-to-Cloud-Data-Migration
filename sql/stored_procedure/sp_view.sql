USE gold_db
GO

CREATE OR ALTER PROC CreateSQLServerlessView_gold @ViewName nvarchar(100)
                AS
                BEGIN

                DECLARE @statement VARCHAR(max)
                        SET @statement = N'CREATE OR ALTER VIEW ' + @ViewName + ' AS
                        SELECT *
                        FROM
                            OPENROWSET(BULK ''https://datalakesagen2.dfs.core.windows.net/gold/public/' + @ViewName + '/'',
                                       FORMAT = ''DELTA''
                            )
                            AS [result]'
EXEC (@statement)
END
GO