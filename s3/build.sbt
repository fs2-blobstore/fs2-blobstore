name := "s3"

val fs2Version = "2.4.3"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                   % "2.13.62",
  "co.fs2"                %% "fs2-reactive-streams" % fs2Version
)
