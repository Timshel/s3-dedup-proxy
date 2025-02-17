val scala3Version = "3.5.2"

enablePlugins(JavaAppPackaging)

lazy val root = project
  .in(file("."))
  .settings(
    name         := "s3-dedup-proxy",
    version      := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-language:higherKinds",
      "-explain",
    ),
    javaOptions += "-Duser.timezone=UTC",
    Compile / mainClass := Some("timshel.s3dedupproxy.Application"),
    unmanagedSources / excludeFilter := HiddenFileFilter
      || "EncryptedBlobStore.java"
      || "S3ProxyExtension.java"
      || "S3ProxyRule.java"
      || "Main.java",
    libraryDependencies ++= Seq(
      // Deps from S3Proxy
      "com.azure"                        % "azure-identity"             % "1.15.0",
      "com.azure"                        % "azure-storage-blob"         % "12.29.0",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml"     % "2.18.2",
      "com.google.auto.service"          % "auto-service"               % "1.1.1",
      "com.google.code.findbugs"         % "annotations"                % "3.0.1",
      "com.google.code.findbugs"         % "jsr305"                     % "3.0.2",
      "com.google.jimfs"                 % "jimfs"                      % "1.3.0",
      "org.apache.commons"               % "commons-fileupload2-javax"  % "2.0.0-M2",
      "org.apache.jclouds"               % "jclouds-allblobstore"       % "2.7.0",
      "org.apache.jclouds.api"           % "filesystem"                 % "2.7.0",
      "org.apache.jclouds.driver"        % "jclouds-slf4j"              % "2.7.0",
      "org.eclipse.jetty"                % "jetty-servlet"              % "11.0.24",

      // Leftovers
      "com.squareup.okhttp3"             % "okhttp"                     % "4.11.0",
      "com.squareup.okhttp3"             % "okhttp-brotli"              % "4.11.0",

      // Java
      "org.flywaydb"                     % "flyway-database-postgresql" % "11.0.1",
      "org.postgresql"                   % "postgresql"                 % "42.7.4",
      "org.slf4j"                        % "slf4j-api"                  % "2.0.16",
      "org.slf4j"                        % "slf4j-simple"               % "2.0.16",

      // Scala
      "com.github.pureconfig"           %% "pureconfig-generic-scala3"  % "0.17.8",
      "com.typesafe.scala-logging"      %% "scala-logging"              % "3.9.5",
      "org.scalikejdbc"                 %% "scalikejdbc"                % "4.3.+",
      "org.tpolecat"                    %% "skunk-core"                 % "0.6.4",
      "org.typelevel"                   %% "cats-effect"                % "3.5.7",

      "org.scalameta"                   %% "munit"                      % "1.1.0" % Test,
      "org.typelevel"                   %% "munit-cats-effect"          % "2.0.0" % Test,
    )
  )
