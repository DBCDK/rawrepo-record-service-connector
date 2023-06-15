#!groovy

def workerNode = "devel11"

pipeline {
	agent {label workerNode}
	tools {
		jdk 'jdk11'
		maven 'Maven 3'
	}
	triggers {
		pollSCM("H/03 * * * *")
		upstream(upstreamProjects: "Docker-payara6-bump-trigger",
				threshold: hudson.model.Result.SUCCESS)
	}
	options {
		timestamps()
	}
	stages {
		stage("clear workspace") {
			steps {
				deleteDir()
				checkout scm
			}
		}
		stage("Maven build") {
			steps {
				sh "mvn verify pmd:pmd pmd:cpd spotbugs:spotbugs"

				junit testResults: '**/target/surefire-reports/TEST-*.xml,**/target/failsafe-reports/TEST-*.xml'

				script {
					def java = scanForIssues tool: [$class: 'Java']
					publishIssues issues: [java], unstableTotalAll:1

					def pmd = scanForIssues tool: [$class: 'Pmd']
					publishIssues issues: [pmd], unstableTotalAll:1
				}
			}
		}
		stage("deploy") {
			when {
				branch "master"
			}
			steps {
				withMaven(maven: 'Maven 3') {
					sh "mvn jar:jar deploy:deploy"
				}
			}
		}
	}
}
