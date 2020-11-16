# NikeSparkTestTask
Please install maven to build job. 3.6.1
Build spark job uber jar.
```
mvn clean package
```
Run on Spark Cluster. Using https://spark.apache.org/docs/latest/submitting-applications.html
Please check artifact version from pom.xml
Please specify your won log4j.xml and applicaiton.conf.
```
/bin/spark-submit \
                --class org.mhr.spark.Main \
                --master <master-url> \
                --deploy-mode <deploy-mode> \
                --conf spark.driver.extraJavaOptions='-Dlog4j.configuration=log4j.xml -Dconfig.file=application.conf' \
                --files your/path/to/log4j.xml,your/path/application.conf \
                target/spark-nike-job-0.1.jar \

```
