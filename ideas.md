12/02/24
- Add ability to chain cleaning funcitons to avoid repeated function calls:
'df = dp.function(df)'.
- Add 'query' argument to 'read_all_sqlite' so that SQL statements can be
modified.
- Add wider array of cleaning options (e.g. values to snakecase, values to 
boolean, values to datetime, values to category) -> saves on memory and reduces
errors in pipelines.