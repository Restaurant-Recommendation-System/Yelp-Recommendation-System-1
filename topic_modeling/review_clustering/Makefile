CLASSPATH = "lib/spark-core_2.11-2.2.0.jar:lib/spark-tags_2.11-2.2.0.jar:lib/hadoop-common-2.6.5.jar:lib/hadoop-annotations-2.6.5.jar:lib/jackson-annotations-2.6.5.jar"
TARGET = target
SCALA_HOME = /Users/rashmidwaraka/Desktop/MR/scala-2.11.8
SPARK_HOME = /usr/local/Cellar/apache-spark/2.2.0

#Data Subset
#INPUT1 = input/doc_topic_sample.txt
#INPUT1 = input/filtered_review.csv
INPUT1 = input/filtered_review.csv
INPUT2 = input/doc_topic.txt

#INPUT3 = input/MillionSongSubset/similar_artists.csv

OUTPUT = output/

all: clean build run

clean:
	-rm -rf $(TARGET)
	-mkdir $(TARGET)

run: compile
	$(SPARK_HOME)/bin/spark-submit --class org.neu.Clustering --master local --driver-memory 8g TopicClustering.jar $(INPUT1) $(INPUT2) $(OUTPUT)
	#$(SPARK_HOME)/bin/spark-submit --class org.neu.TopicClustering --master local --driver-memory 8g TopicClustering.jar $(INPUT1) $(OUTPUT)

compile: clean
	$(SCALA_HOME)/bin/scalac -classpath $(CLASSPATH) -d $(TARGET) src/main/scala/org/neu/*.scala

build: compile
	jar cvfm TopicClustering.jar MANIFEST.MF -C target/ .
