# fireband

[![Project Status: Abandoned - Initial development has started, but there has not yet been a stable, usable release; the project has been abandoned and the author(s) do not intend on continuing development.](http://www.repostatus.org/badges/latest/abandoned.svg)](http://www.repostatus.org/#abandoned)

Firebase bandwidth analyzer

Currently includes only a NodeJS collector that captures read/write sizes by path and sends them to BigQuery tables (sharded by day).

Just a first step, lots more to come.  You probably don't want to use this yet!

Sample query:

```
SELECT op, path, tag, SUM(size) AS total_size FROM (
  SELECT op, REGEXP_EXTRACT(path, r'^(/[^/]*)') AS path, tag, size
  FROM TABLE_DATE_RANGE(bandwidth_dev.raw, DATE_ADD(CURRENT_TIMESTAMP(), -7, 'DAY'), CURRENT_TIMESTAMP())
) GROUP BY op, path, tag ORDER BY total_size DESC
```
