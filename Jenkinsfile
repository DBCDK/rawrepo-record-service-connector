#!groovy

def workerNode = "devel8"

pipeline {
	agent {label workerNode}
	triggers {
		pollSCM("H/03 * * * *")
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
		stage("verify") {
			steps {
				withMaven(maven: 'Maven 3') {
					sh "mvn verify pmd:pmd javadoc:aggregate"
					junit "target/surefire-reports/TEST-*.xml"
				}
			}
		}
		stage("warnings") {
			agent {label workerNode}
			steps {
				warnings consoleParsers: [
					[parserName: "Java Compiler (javac)"],
					[parserName: "JavaDoc Tool"]
				],
					unstableTotalAll: "0",
					failedTotalAll: "0"
			}
		}
		stage("pmd") {
			agent {label workerNode}
			steps {
				step([$class: 'hudson.plugins.pmd.PmdPublisher',
					  pattern: 'target/pmd.xml',
					  unstableTotalAll: "0",
					  failedTotalAll: "0"])
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
