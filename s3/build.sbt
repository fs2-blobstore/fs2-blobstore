name := "s3"

val fs2Version = "3.2.4"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.17.114",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "software.amazon.awssdk" % "s3-transfer-manager"       % "2.17.110-PREVIEW" % Test,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.39.12"          % Test
)
