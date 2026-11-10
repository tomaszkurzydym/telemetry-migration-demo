// SYNTHETIC DEMO legacy maintenance-sync job. Toolchain pins: Scala 2.12.18, Spark 3.5.7, JDK 17, sbt 1.10.7.
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "demo.synthetic"

val sparkVersion = "3.5.7"

lazy val root = (project in file("."))
  .settings(
    name := "legacy-maintenance-sync",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"            % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.postgresql"     % "postgresql"           % "42.7.4",
      "org.scalatest"    %% "scalatest"            % "3.2.19" % Test
    ),
    fork := true,
    run / connectInput := true,
    outputStrategy := Some(StdoutOutput),
    javaOptions ++= Seq(
      "-Xmx2g",
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
  )
