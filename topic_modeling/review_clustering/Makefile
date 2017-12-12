CLASSPATH = "lib/spark-core_2.11-2.2.0.jar:lib/spark-tags_2.11-2.2.0.jar:lib/hadoop-common-2.6.5.jar:lib/hadoop-annotations-2.6.5.jar:lib/jackson-annotations-2.6.5.jar"
TARGET = target
SCALA_HOME = /Users/rashmidwaraka/Desktop/MR/scala-2.11.8
SPARK_HOME = /usr/local/Cellar/apache-spark/2.2.0

#Data Subset
INPUT1 = input/business_ratings_topic_wise.csv
INPUT2 = input/business_ratings_food_type.csv
INPUT3 = input/filtered_business.csv

OUTPUT = input/

all: clean build run

clean:
	-rm -rf $(TARGET)
	-mkdir $(TARGET)

run: compile
	$(SPARK_HOME)/bin/spark-submit --class org.neu.recommend.recommendations --master local --driver-memory 12g DataTransfer.jar $(INPUT1) $(INPUT2) $(INPUT3) $(OUTPUT)
	#$(SPARK_HOME)/bin/spark-submit --class org.neu.data_transform.topic_rating_transform --master local --driver-memory 12g DataTransfer.jar $(INPUT1) $(OUTPUT)
	#$(SPARK_HOME)/bin/spark-submit --class org.neu.data_transform.review_topic_transform --master local --driver-memory 12g DataTransfer.jar $(INPUT1) $(INPUT2) $(OUTPUT)
	#$(SPARK_HOME)/bin/spark-submit --class org.neu.TopicClustering --master local --driver-memory 8g TopicClustering.jar $(INPUT3) $(OUTPUT)


compile: clean
	$(SCALA_HOME)/bin/scalac -classpath $(CLASSPATH) -d $(TARGET) src/main/scala/org/neu/**/*.scala

build: compile
	jar cvfm DataTransfer.jar MANIFEST.MF -C target/ .
