name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.1.9",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.16" % Test
)
