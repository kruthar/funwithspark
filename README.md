# funwithspark
A collection of Spark examples

To build:

```
sbt clean package
```

To Run Weather example:

```
[SPARK_HOME]/bin/spark-submit --master local[*] --class com.kruth.scala.Weather target/scala-2.11/funwithspark_2.11-0.1.jar
```

File for com.kruth.scala.Weather can be found at ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/gsn/AE000041196.dly
Just download into top level of this project.
