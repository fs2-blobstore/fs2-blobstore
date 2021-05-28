import microsites.ConfigYml

micrositeName := "fs2-blobstore"
micrositeDescription := "Unified fs2 interface for various storage services"
micrositeAuthor := "fs2-blobstore"
micrositeGithubOwner := "jgogstad"
micrositeGithubRepo := "fs2-blobstore"
micrositeBaseUrl := "fs2-blobstore"
micrositeDocumentationUrl := "documentation/data-model"
micrositeShareOnSocial := false
micrositeGithubLinks := false
micrositeGitterChannel := false
micrositeSearchEnabled := false
micrositeHomeButtonTarget := "docs"
micrositeFooterText := None

mdocIn := sourceDirectory.value / "main" / "mdoc"
mdocExtraArguments := List("--no-link-hygiene")
mdocVariables := Map(
  "stableVersion" -> dynverGitPreviousStableVersion.value.get.version,
  "scalaVersions" -> crossScalaVersions.value.flatMap(CrossVersion.partialVersion).map(_._2).mkString("2.", "/", "")
)

publish / skip := true
Compile / scalacOptions -= "-Wdead-code"