name := "s3"

val fs2Version = "3.1.6"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.17.65",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.39.9" % Test
)
