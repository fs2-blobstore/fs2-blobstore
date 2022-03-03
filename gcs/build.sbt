name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.4.4",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.20" % Test
)
