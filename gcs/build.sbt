name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.50.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.33" % Test
)
