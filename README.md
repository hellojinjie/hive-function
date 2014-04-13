hive-function
=============

hive udf/udaf/udtf functions

Usage
=
Add jars
````
add file file:///home/qos/GeoLite2-City.mmdb;
add jar hdfs:/user/jj/geo/jackson-core-2.2.0.jar;
add jar hdfs:/user/jj/geo/jackson-annotations-2.2.0.jar;
add jar hdfs:/user/jj/geo/jackson-databind-2.2.0.jar;
add jar hdfs:/user/jj/geo/maxminddb-0.3.0.jar;
add jar hdfs:/user/jj/geo/geoip2-0.6.0.jar;
add jar hdfs:/user/jj/hive/hive-function-1.0-SNAPSHOT.jar;
````

Create temporary function
````
create temporary function shaman_geo_countrycode as 'shaman.hive.udf.UDFGeoIPCountryCode';
create temporary function shaman_geo_countryname as 'shaman.hive.udf.UDFGeoIPCountryName';
create temporary function shaman_geo_isp as 'shaman.hive.udf.UDFGeoIPIsp';
````

#### Example usage of geo udf
````
create temporary function shaman_geo_countrycode as 'shaman.hive.udf.UDFGeoIPCountryCode';
select shaman_geo_countrycode('54.206.1.110') from jj.dual;
````

#### Example usage of map value sum
````
add jar ./target/hive-function-1.0-SNAPSHOT.jar;
create temporary function shaman_sum_mapvalue as 'shaman.hive.udf.UDFMapValueSum';
select shaman_sum_mapvalue(map('a', 344, 'b', 4444)) from jj.dual limit 1;
select shaman_sum_mapvalue(map('a', 344444444444, 'b', 44444444444444)) from jj.dual limit 1;
````

#### Example usage of array value sum
````
add jar hdfs:///user/jj/hive/hive-function-1.0-SNAPSHOT.jar;
create temporary function shaman_sum_array as 'shaman.hive.udf.UDFArraySum';
select shaman_sum_array(array(1,2,3,4)) from jj.dual;
select shaman_sum_array(array(1,2,3,411111111111111111)) from jj.dual;
````