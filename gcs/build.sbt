name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "1.111.2",
  "com.google.cloud" % "google-cloud-nio"     % "0.122.1" % Test
)
