spawner:
	mvn exec:java -Dexec.mainClass=runnable.Spawner

client:
	mvn exec:java -Dexec.mainClass=runnable.Client -Dexec.args="4565"

build:
	mvn clean package

clearlogs:
	truncate -s 0 logs/*.log