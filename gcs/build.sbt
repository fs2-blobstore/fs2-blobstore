name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.62.1",
  "com.google.cloud" % "google-cloud-nio"     % "0.128.9" % Test
)
