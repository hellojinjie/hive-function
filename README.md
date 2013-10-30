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
