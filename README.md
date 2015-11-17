# funwithspark
A collection of Spark examples

## To build:

Run sbt in the top level project
```sh
sbt clean package
```

## Examples
### Weather

```sh
[SPARK_HOME]/bin/spark-submit --master local[*] --class com.kruth.spark.examples.weather.Weather target/scala-2.11/funwithspark_2.11-0.1.jar data/
```

Files for the weather example can be found at ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/gsn
Just download into a data directory and specify the path in the spark-submit command.
