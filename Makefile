spawner:
	mvn exec:java -Dexec.mainClass=runnable.Spawner

client:
	mvn exec:java -Dexec.mainClass=runnable.Client

build:
	mvn clean package

clearlogs:
	truncate -s 0 logs/*.log