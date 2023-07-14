name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.23.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.126.19" % Test
)
