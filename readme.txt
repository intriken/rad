I wrote everything in Scala/Spark using hive formatted tables in hadoop as the export.
scripts are setup to run on a local environment.

spark was installed on my local machine from the download here:
https://spark.apache.org/downloads.html


---HOW TO RUN SCRIPTS---

--Run the Script to populate the BIKE_INFO table from CSVs
spark-shell -i serial_process.scala
