name := "s3"

val fs2Version = "3.9.2"
val awsVersion = "2.20.153"

libraryDependencies ++= Seq(
  "software.amazon.awssdk"     % "s3"                        % awsVersion,
  "co.fs2"                    %% "fs2-reactive-streams"      % fs2Version,
  "software.amazon.awssdk.crt" % "aws-crt"                   % "0.27.1" % Test,
  "com.dimafeng"              %% "testcontainers-scala-core" % "0.41.0" % Test
)
