CODE="Weather"
JAR="weather_NYC"

# Erase the hdfs directory
# hdfs dfs -rm -r -f weather

hdfs dfs -rm -r -f project/output-weather

# Setup the HDFS directory stucture and populate it. 

# Create new directory structure in HDFS
# hdfs dfs -mkdir weather

# Populate the input directory in HDFS with the input file
# hdfs dfs -put input.txt weather

# Verify what is in the input data directory
hdfs dfs -ls project


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
hadoop jar ${JAR}.jar ${CODE} /user/"$USER"/project/nyc_weather_2018-2019.3.csv /user/"$USER"/project/output-weather


# Output the result (assuming 1 reducer)
hdfs dfs -cat project/output-weather/part-r-00000
