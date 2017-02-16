# BDPA

These are the assignments for CentraleSupelec course "Big data processing and analytics", using Hadoop MapReduce and HDFS.

## Configuration used

Hadoop 2.7.3 pseudo distributed on MacOS, with native libraries installed.

## Assignment 1



### StopWords

Retrieve StopWords (# occurences > 4000) in a corpus of documents.

1. Import data into HDFS, in  `~/StopWords/input`
2. Compile java file: `javac StopWords.java -cp $(hadoop classpath)`
3. Compile JAR : `jar cf sw.jar StopWords*.class`
4. Run JAR : `hadoop jar sw.jar StopWords StopWords/input StopWords/output`
5. Concatenate output : `hadoop fs -cat StopWords/output/part\* | hadoop fs -put - StopWords/stopwords.csv`



### InvertedIndex

Shows the list of documents in which words (excluding stop words) appear.

1. Compile java file: `javac InvertedIndex.java -cp $(hadoop classpath)`
2. Compile JAR : `jar cf ii.jar InvertedIndex*.class`
3. Run JAR : `hadoop jar ii.jar InvertedIndex StopWords/input InvertedIndex/output StopWords/stopwords.csv`

### NewInvertedIndex

Shows the list of documents in which words (excluding stop words) appear, and the number of occurences in each document.

1. Compile java file: `javac NewInvertedIndex.java -cp $(hadoop classpath)`
2. Compile JAR : `jar cf ii.jar NewInvertedIndex*.class`
3. Run JAR : `hadoop jar ii.jar NewInvertedIndex StopWords/input InvertedIndex/output StopWords/stopwords.csv`
