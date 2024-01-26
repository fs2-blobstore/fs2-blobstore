name := "s3"

val fs2Version = "3.9.4"
val awsVersion = "2.21.46"

libraryDependencies ++= Seq(
  "software.amazon.awssdk"     % "s3"                        % awsVersion,
  "co.fs2"                    %% "fs2-reactive-streams"      % fs2Version,
  "software.amazon.awssdk.crt" % "aws-crt"                   % "0.28.13" % Test,
  "com.dimafeng"              %% "testcontainers-scala-core" % "0.41.0"  % Test
)
