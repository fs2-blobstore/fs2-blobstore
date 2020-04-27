name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.107.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.121.0" % Test
)
