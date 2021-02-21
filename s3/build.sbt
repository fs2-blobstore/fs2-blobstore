name := "s3"

val fs2Version = "2.5.3"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.15.81",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.39.1" % Test
)
