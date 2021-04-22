name := "s3"

val fs2Version = "3.0.1"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.16.45",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.39.3" % Test
)
