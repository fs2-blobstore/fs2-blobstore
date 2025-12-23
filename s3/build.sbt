name := "s3"

val fs2Version = "3.12.2"
val awsVersion = "2.40.7"

libraryDependencies ++= Seq(
  "software.amazon.awssdk"     % "s3"                        % awsVersion,
  "co.fs2"                    %% "fs2-reactive-streams"      % fs2Version,
  "software.amazon.awssdk.crt" % "aws-crt"                   % "0.40.3" % Test,
  "com.dimafeng"              %% "testcontainers-scala-core" % "0.44.1" % Test
)
