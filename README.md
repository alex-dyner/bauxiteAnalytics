

A small test application to show the capabilities of SQL in the solution of a sessionize events task.
All data transformation operations are implemented in **pure SQL**.
It is not completely type safety, but it is important for fast code development.

The application transforms the data into one of the possible modes.
Supported modes:
SessionizedEvents - add to any rows from input 3 extra field 
    - session id - string based unique session identifier
    - session start time - string image of session start timestamp (with ISO 8601 format) 
    - session end time - string image of session end timestamp (with ISO 8601 format) 
Note: if case when session have only one event start time been equal end time
    
MedianSessionDurationByCategory - for each category find median session (by normal session definition) duration 
DurationHistogramByCategory - for each category find number of unique users spending less than 1 min, 1 to 5 mins and more than 5 mins
Top10ProductByCategory - for each category find top 10 products ranked by time spent by users on product pages.


**For build project use sbt assembly**
sbt assembly

**For run locally use command pattern
java -Dspark.master=<master_option> -jar <SessionizedEvents|MedianSessionDurationByCategory|DurationHistogramByCategory|Top10ProductByCategory> <path to JAR file> <path to source event data>

Example: 
java -Dspark.master=local[2] -jar /tmp/bauxiteAnalytics/app/bauxiteSalesReport-assembly-0.1.jar SessionizedEvents /tmp/bauxiteAnalytics/in/sample_of_input_data.csv /tmp/bauxiteAnalytics/out/SessionizedEvents/
ls /tmp/bauxiteAnalytics/out/SessionizedEvents/
cat /tmp/bauxiteAnalytics/out/SessionizedEvents//p*

**For run on cluster use command pattern
spark-submit --class com.bauxite.reporting.Main --master <master_definition> <path_to_JAR> <run_mode> <input_data> <output_data>
Example:
spark-submit --class com.bauxite.reporting.Main --master http://localhost:8080 /tmp/bauxiteSalesReport-assembly-0.1.jar MedianSessionDurationByCategory /tmp/data /tmp/out


