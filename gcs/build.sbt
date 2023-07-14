name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.24.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.126.18" % Test
)
