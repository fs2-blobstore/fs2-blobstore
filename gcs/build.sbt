name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.117.1",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.6" % Test
)
