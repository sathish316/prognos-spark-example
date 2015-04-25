name := "prognos-spark-example"
 
version := "1.0"
 
scalaVersion := "2.10.5"

resolvers += "Local Ivy Repository" at "file://"+Path.userHome.absolutePath+"/.ivy2/local"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.0"

libraryDependencies += "com.fk" % "prognos_2.10" % "1.0"
