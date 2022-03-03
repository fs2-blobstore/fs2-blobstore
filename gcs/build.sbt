name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.4.2",
  "com.google.cloud" % "google-cloud-nio"     % "0.123.22" % Test
)
