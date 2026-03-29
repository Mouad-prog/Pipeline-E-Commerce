FROM eclipse-temurin:17-jre-focal

# Set working directory
WORKDIR /app

# Add Hadoop environment variables and native libs so Spark works properly
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH

# We don't strictly need windows winutils inside the linux container, 
# but setting HADOOP_HOME prevents certain Spark warnings.

# Ensure output directories exist inside container
RUN mkdir -p /app/output/parquet /app/output/reports /app/output/checkpoints

# Copy the fat jar built by sbt assembly
COPY target/scala-2.12/ecommerce-pipeline.jar /app/ecommerce-pipeline.jar

# Run the orchestrator in demo mode
ENTRYPOINT ["java", "-cp", "ecommerce-pipeline.jar", "orchestration.PipelineOrchestrator", "--mode", "demo"]
