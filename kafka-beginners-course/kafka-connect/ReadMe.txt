#How to run connector
#Go to path where we kept properties files and open cmd
G:\git\apacheKafkaRepo\kafka-beginners-course\kafka-connect

#Now run below command
#Here connect-standalone.properties is copied from kafka->config folder
#twitter.properties was created as described in git https://github.com/jcustenborder/kafka-connect-twitter/tree/0.2.26
connect-standalone.bat connect-standalone.properties twitter.properties