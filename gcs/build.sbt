name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.52.3",
  "com.google.cloud" % "google-cloud-nio"     % "0.127.37" % Test
)
