CODE="Weather"
JAR="weather_NYC"

# Erase the hdfs directory
hdfs dfs -rm -r -f weather

# Setup the HDFS directory stucture and populate it. 

# Create new directory structure in HDFS
hdfs dfs -mkdir weather
hdfs dfs -mkdir weather/output-weather

# Populate the input directory in HDFS with the input file
hdfs dfs -put nyc_weather_2018-2020.3.csv weather

# Verify what is in the input data directory
hdfs dfs -ls weather


# Build the jar file and run

# Remove class and jar files
rm *.class
rm *.jar

# Compile
javac -classpath `yarn classpath` -d . ${CODE}Mapper.java
javac -classpath `yarn classpath` -d . ${CODE}Reducer.java
javac -classpath `yarn classpath`:. -d . ${CODE}.java

# Create jar file
jar -cvf ${JAR}.jar *.class

# Run the program
hadoop jar ${JAR}.jar ${CODE} /user/"$USER"/weather/nyc_weather_2018-2020.3.csv /user/"$USER"/weather/output-weather


# Output the result (assuming 1 reducer)
hdfs dfs -cat weather/output-weather/part-r-00000
