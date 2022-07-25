name := "s3"

val fs2Version = "3.2.10"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.17.238",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "software.amazon.awssdk" % "s3-transfer-manager"       % "2.17.238-PREVIEW" % Test,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.40.9"           % Test
)
