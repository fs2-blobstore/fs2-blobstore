name := "s3"

val fs2Version = "3.0.4"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.16.84",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.39.5" % Test
)
