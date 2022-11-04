FROM airbyte/worker:0.40.4

ARG DOCKER_BUILD_ARCH=amd64

ARG VERSION=0.40.4

WORKDIR /app

# copy instrumentation to /app
ADD airbyte-instrumentation/target/airbyte-instrumentation.jar /app

# copy agent lib to /app
ADD airbyte-agent/target/airbyte-agent.jar /app
# Prepend the agent jar to the existing classpath def (there is no other way to extend it)
RUN sed -i 's|^CLASSPATH=|CLASSPATH=/app/airbyte-agent.jar:|g' /app/airbyte-workers-0.40.4/bin/airbyte-workers 

# Add the following 2 lines  to enter in debug mode
# In JAVA_OPTS => -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:9044
# After => EXPOSE 9044
ENV JAVA_OPTS -javaagent:/app/airbyte-instrumentation.jar

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "${APPLICATION}-${VERSION}/bin/${APPLICATION}"]
