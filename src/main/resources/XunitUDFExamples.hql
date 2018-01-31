ADD JAR hdfs:///user/leungc/lib/brickhouse-0.7.19-JS.jar;
CREATE TEMPORARY FUNCTION get_ypath AS 'brickhouse.udf.xunit.GetYPathUDF';
CREATE TEMPORARY FUNCTION remove_ypath AS 'brickhouse.udf.xunit.RemoveYPathUDF';

CREATE TEMPORARY FUNCTION getxunitsize AS 'brickhouse.udf.xunit.GetXUnitSizeUDF';
CREATE TEMPORARY FUNCTION getallypaths AS 'brickhouse.udf.xunit.GetAllYPathsUDF';

CREATE TEMPORARY FUNCTION getypathvalue AS 'brickhouse.udf.xunit.GetYPathValueUDF';




select get_ypath("/brand", "/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com");
--/brand/brand='47

select remove_ypath("/brand", "/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com");
--/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com

select getxunitsize("/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com");
--3

select getallypaths("/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com");
--brand,category,domain

select getallypaths("/category/top_level=Appliances/level1=Kitchen Appliances/level2=Kitchen Appliance Parts & Accessories");
--category


select getypathvalue("brand", "/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com");
--'47'

select getypathvalue("category", "/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com");
--Women's Clothing > Socks & Hosiery

select getypathvalue("domain", "/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com");
--nordstrom.com


SELECT getypathvalue("category", xunit) categorytree
FROM funnel_cube.funnel_aggregates_uncalibrated
WHERE as_of=20171231
AND getallypaths(xunit) = "category"
GROUP BY getypathvalue("category", xunit)
ORDER BY categorytree
;

select xunit
FROM funnel_cube.funnel_aggregates_uncalibrated
WHERE as_of=20171231
AND xunit like '%Sports  Fitness%'
limit 10
;
