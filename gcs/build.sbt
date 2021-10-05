name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.1.7",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.10" % Test
)
