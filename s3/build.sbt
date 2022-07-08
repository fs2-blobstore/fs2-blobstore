name := "s3"

val fs2Version = "3.2.9"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.17.189",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "software.amazon.awssdk" % "s3-transfer-manager"       % "2.17.189-PREVIEW" % Test,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.40.8"           % Test
)
