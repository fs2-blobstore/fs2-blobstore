name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.25.0",
  "com.google.cloud" % "google-cloud-nio"     % "0.126.19" % Test
)
