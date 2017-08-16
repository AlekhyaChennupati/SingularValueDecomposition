name := "Simple Project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++=Seq("org.apache.spark" %% "spark-core" % "1.2.1",
"org.scalanlp" %% "breeze" % "0.10","org.scalanlp" %% "breeze-natives" % "0.10"
)

resolvers ++= Seq("Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)
