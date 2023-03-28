name := "s3"

val fs2Version = "3.6.1"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.19.33",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "software.amazon.awssdk" % "s3-transfer-manager"       % "2.17.292-PREVIEW" % Test,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.40.14"          % Test
)
