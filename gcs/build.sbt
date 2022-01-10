name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.2.3",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.18" % Test
)
