name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.58.1",
  "com.google.cloud" % "google-cloud-nio"     % "0.128.6" % Test
)
