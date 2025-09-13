name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.56.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.128.4" % Test
)
