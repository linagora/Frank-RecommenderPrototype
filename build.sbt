name := "RecommenderPrototype"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"              % "1.3.0",
  "org.apache.spark"  %% "spark-mllib"              % "1.3.0",
  "org.apache.spark"  %% "spark-sql"               % "1.3.0",
  "org.apache.spark"  %% "spark-mllib"             % "1.3.0")


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += Resolver.sonatypeRepo("public")