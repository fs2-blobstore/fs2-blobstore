name := "gcs"

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-storage" % "2.22.3",
  "com.google.cloud" % "google-cloud-nio"     % "0.126.15" % Test
)
