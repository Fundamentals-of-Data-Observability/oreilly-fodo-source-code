export CLASSPATH=/Users/andy/Library/Caches/Coursier/v1/https//repo1.maven.org/maven2/org/ow2/asm/asm/9.3/asm-9.3.jar
export CLASSPATH=$CLASSPATH:/Users/andy/Library/Caches/Coursier/v1/https//repo1.maven.org/maven2/org/ow2/asm/asm-util/9.3/asm-util-9.3.jar

export CLASSPATH=$CLASSPATH:/Users/andy/Library/Caches/Coursier/v1/https/airbyte.mycloudrepo.io/public/repositories/airbyte-public-jars/io/airbyte/airbyte-workers/0.40.4/airbyte-workers-0.40.4.jar
# export CLASSPATH=$CLASSPATH:/Users/andy/kensu/clients/asm-helpers/asm-host/target/asm-host.jar

java -cp $CLASSPATH org.objectweb.asm.util.ASMifier io.airbyte.workers.general.DefaultReplicationWorker > ./airbyte-instrumentation/src/main/scripts/DefaultReplicationWorker.java
# java -cp $CLASSPATH org.objectweb.asm.util.ASMifier io.kensu.sandbox.toasmify.ASMUseInput > ./asm-host/src/main/scripts/Generated.java