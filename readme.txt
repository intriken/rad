I wrote everything in Scala/Spark using hive formatted tables in hadoop as the export.
scripts are setup to run on a local environment.

spark was installed on my local machine from the download here:
https://spark.apache.org/downloads.html

I loaded the decoding information into CSV files in the decode folder

---HOW TO RUN SCRIPTS---

--Run the Script to populate the BIKE_INFO table from CSVs
spark-shell -i serial_process.scala


--PROCESS--
The script will load the files from serial_drive directory load and parse them remove any duplicates.

After that it loads the current table to verify if the serial_num is new if not it will not write the new entries preventing duplicates in the future.

Then all the files in the serial_drive are moved to the serial_drive_completed directory

--OUTPUTS--
A Hive formatted table in hdfs files are crated named BIKE_INFO

I also outputed a CSV of the table to the output directory
