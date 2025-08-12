name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.53.2",
  "com.google.cloud" % "google-cloud-nio"     % "0.128.2" % Test
)
