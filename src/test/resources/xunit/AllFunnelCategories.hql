ADD JAR hdfs:///user/leungc/lib/brickhouse-0.7.19-JS.jar;
CREATE TEMPORARY FUNCTION get_ypath AS 'brickhouse.udf.xunit.GetYPathUDF';
CREATE TEMPORARY FUNCTION remove_ypath AS 'brickhouse.udf.xunit.RemoveYPathUDF';

CREATE TEMPORARY FUNCTION getxunitsize AS 'brickhouse.udf.xunit.GetXUnitSizeUDF';
CREATE TEMPORARY FUNCTION getallypaths AS 'brickhouse.udf.xunit.GetAllYPathsUDF';

CREATE TEMPORARY FUNCTION getypathvalue AS 'brickhouse.udf.xunit.GetYPathValueUDF';

SELECT getypathvalue("category", xunit) categorytree
FROM funnel_cube.funnel_aggregates_uncalibrated
WHERE as_of=20171231
AND getallypaths(xunit) = "category"
GROUP BY getypathvalue("category", xunit)
ORDER BY categorytree
;
