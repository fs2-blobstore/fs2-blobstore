name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.112.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.121.2" % Test
)
