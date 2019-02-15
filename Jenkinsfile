pipeline {
    agent {
       kubernetes {
              // Change the name of jenkins-maven label to be able to use yaml configuration snippet
              label "maven-dind"
              // Inherit from Jx Maven pod template
              inheritFrom "maven-java11"
              // Add pod configuration to Jenkins builder pod template
              yamlFile "maven-dind.yaml"
            }
    }
    environment {
      ORG               = 'activiti'
      APP_NAME          = 'ttc-connectors-ranking'
      CHARTMUSEUM_CREDS = credentials('jenkins-x-chartmuseum')
    }
    stages {
      stage('CI Build and push snapshot') {
        when {
          branch 'PR-*'
        }
        environment {
          PREVIEW_VERSION = "0.0.0-SNAPSHOT-$BRANCH_NAME-$BUILD_NUMBER"
          PREVIEW_NAMESPACE = "$APP_NAME-$BRANCH_NAME".toLowerCase()
          HELM_RELEASE = "$PREVIEW_NAMESPACE".toLowerCase()
        }
        steps {
          container('maven') {
            sh "mvn versions:set -DnewVersion=$PREVIEW_VERSION"
            sh "mvn install"
            sh 'export VERSION=$PREVIEW_VERSION && skaffold build -f skaffold.yaml'
          }
        }
      }
      stage('Build Release') {
        when {
          branch '7.0.x'
        }
        steps {
          container('maven') {
            // ensure we're not on a detached head
            sh "git checkout 7.0.x"
            sh "git config --global credential.helper store"
            sh "jx step validate --min-jx-version 1.1.73"
            sh "jx step git credentials"
            // so we can retrieve the version in later steps
            sh "echo \$(jx-release-version) > VERSION"
            sh "mvn versions:set -DnewVersion=\$(cat VERSION)"
          }
          dir ('./charts/ttc-connectors-ranking') {
            container('maven') {
              sh "make tag"
            }
          }
          container('maven') {
            sh 'mvn clean deploy'

            sh 'export VERSION=`cat VERSION` && skaffold build -f skaffold.yaml'
            sh "jx step post build --image $DOCKER_REGISTRY/$ORG/$APP_NAME:\$(cat VERSION)"
            //sh './updatebot.sh'
          }
        }
      }
      stage('Promote to Environments') {
        when {
          branch '7.0.x'
        }
        steps {
          dir ('./charts/ttc-connectors-ranking') {
            container('maven') {
              //sh 'jx step changelog --version v\$(cat ../../VERSION)'
              // release the helm chart
              //sh 'make release'
              // promote through all 'Auto' promotion Environments
              //sh 'jx promote -b --all-auto --timeout 1h --version \$(cat ../../VERSION)'
            }
          }
        }
      }
    }
    post {
        always {
            cleanWs()
        }
    }
  }
