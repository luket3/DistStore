spawner:
	mvn exec:java -Dexec.mainClass=runnable.Spawner

spawner-fresh:
	mvn exec:java -Dexec.mainClass=runnable.Spawner -Dexec.args="--fresh"

client:
	mvn exec:java -Dexec.mainClass=runnable.Client -Dexec.args="4565"

build:
	mvn clean package

test:
	mvn test

# end-to-end tests that boot real server processes; add IT_ARGS=-Dit.keep=true to keep their logs
integration:
	mvn test -Dgroups=integration -DexcludedGroups= $(IT_ARGS)

clearlogs:
	truncate -s 0 logs/*.log