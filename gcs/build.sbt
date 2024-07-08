name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.40.1",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.20" % Test
)
