name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.68.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.132.0" % Test
)
