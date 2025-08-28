name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.53.3",
  "com.google.cloud" % "google-cloud-nio"     % "0.128.3" % Test
)
