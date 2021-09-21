name := "s3"

val fs2Version = "3.1.1"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.17.42",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.39.7" % Test
)
