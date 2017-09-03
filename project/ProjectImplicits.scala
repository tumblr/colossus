import com.typesafe.tools.mima.plugin.MimaPlugin.autoImport.mimaPreviousArtifacts
import sbt.Project

object ProjectImplicits{

  implicit class ProjectExtras(project : Project){
    import sbt._
    import Keys._

    def withMima(versions : String*) : Project = {
      project.settings{
        mimaPreviousArtifacts := (if(scalaBinaryVersion.value == "2.11") {
          versions.map{ v =>
            organization.value %% s"${moduleName.value}" % v
          }.toSet
        }else{
          Set.empty
        })
      }
    }
  }
}

