# graphics-generation
This application is a spark job that retrieves data from Hive database and generates images using the awt java library. 
Images are saved in a specified number of zip files and stored against HDFS.

The project can be executed as follow :

./script_name.sh number_of_records_to_retrieve number_of_partition_desired
