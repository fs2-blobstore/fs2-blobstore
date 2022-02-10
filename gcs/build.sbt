name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.4.1",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.18" % Test
)
