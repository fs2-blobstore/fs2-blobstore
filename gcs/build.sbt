name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.17.2",
  "com.google.cloud" % "google-cloud-nio"     % "0.126.15" % Test
)
