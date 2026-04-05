name := "ecommerce-data-pipeline"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"
val kafkaVersion = "3.6.1"
val circeVersion = "0.14.6"

libraryDependencies ++= Seq(
  // Apache Spark
  "org.apache.spark" %% "spark-core"           % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming"      % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  // Apache Kafka
  "org.apache.kafka"  % "kafka-clients"        % kafkaVersion,
  "org.apache.kafka" %% "kafka"                % kafkaVersion,

  // PostgreSQL JDBC
  "org.postgresql" % "postgresql" % "42.7.1",

  // JSON (Circe)
  "io.circe" %% "circe-core"    % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser"  % circeVersion,

  // Logging
  "ch.qos.logback"              % "logback-classic" % "1.4.14",
  "com.typesafe.scala-logging" %% "scala-logging"   % "3.9.5",

  // Config
  "com.typesafe" % "config" % "1.4.3"
)

// Exclude duplicate SLF4J bindings (Kafka brings log4j-slf4j2-impl)
excludeDependencies ++= Seq(
  ExclusionRule("org.apache.logging.log4j", "log4j-slf4j2-impl")
)

// For running locally with spark embedded
run / fork := true
run / javaOptions ++= Seq(
  "-Djava.library.path=C:\\hadoop\\bin",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)

// Override provided scope for local runs
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// Assembly settings
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _      => MergeStrategy.concat
      case _                    => MergeStrategy.discard
    }
  case "reference.conf"                    => MergeStrategy.concat
  case x if x.endsWith(".proto")           => MergeStrategy.first
  case x if x.contains("hadoop")          => MergeStrategy.first
  case x if x.contains("UnusedStubClass") => MergeStrategy.first
  case _                                   => MergeStrategy.first
}

assembly / assemblyJarName := "ecommerce-pipeline.jar"
