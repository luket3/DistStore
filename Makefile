startservers:
	mvn exec:java -Dexec.mainClass=runnable.StartNodes

startclient:
	mvn exec:java -Dexec.mainClass=runnable.Client

build:
	mvn clean package

clearlogs:
	truncate -s 0 logs/*.log