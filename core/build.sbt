name := "core"

val fs2Version = "3.9.4"

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % fs2Version,
  "co.fs2" %% "fs2-io"   % fs2Version
)
