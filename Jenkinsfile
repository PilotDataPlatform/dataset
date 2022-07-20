pipeline {
    agent { label 'small' }

    environment {
      imagename = "ghcr.io/pilotdataplatform/dataset"
      commit = sh(returnStdout: true, script: 'git describe --always').trim()
      registryCredential = 'pilot-ghcr'
    }

    stages {

    stage('DEV git clone') {
        when {branch "develop"}
        steps{
          script {
          git branch: "develop",
              url: 'https://github.com/PilotDataPlatform/dataset.git',
              credentialsId: 'pilot-gh'
            }
        }
    }

    stage('DEV build and push image') {
      when {branch "develop"}
      steps {
        script {
            sh '''#!/bin/bash
                cd /usr/local/lib/python3.9/
                sudo py3clean -v .
               '''
            docker.withRegistry('https://ghcr.io', registryCredential) {
                customImage = docker.build('$imagename:alembic-$commit', '--target alembic-image .')
                customImage.push()
            }
            docker.withRegistry('https://ghcr.io', registryCredential) {
                customImage = docker.build('$imagename:dataset-$commit', '--target dataset-image .')
                customImage.push()
            }
        }
      }
    }

    stage('DEV: Remove image') {
        when { branch 'develop' }
        steps {
            sh 'docker rmi $imagename:alembic-$commit'
            sh 'docker rmi $imagename:dataset-$commit'
        }
    }

    stage('DEV deploy') {
      when {branch "develop"}
      steps{
      build(job: "/VRE-IaC/UpdateAppVersion", parameters: [
        [$class: 'StringParameterValue', name: 'TF_TARGET_ENV', value: 'dev' ],
        [$class: 'StringParameterValue', name: 'TARGET_RELEASE', value: 'dataset' ],
        [$class: 'StringParameterValue', name: 'NEW_APP_VERSION', value: "$commit" ]
    ])
      }
    }

    stage('STAGING git clone') {
        when {branch "main"}
        steps{
          script {
          git branch: "main",
              url: 'https://github.com/PilotDataPlatform/dataset.git',
              credentialsId: 'pilot-gh'
            }
        }
    }

    stage('STAGING Build and push image') {
      when {branch "main"}
      steps {
        script {
            docker.withRegistry('https://ghcr.io', registryCredential) {
                customImage = docker.build('$imagename:alembic-$commit', '--target alembic-image .')
                customImage.push()
            }
            withCredentials([
                usernamePassword(credentialsId:'minio', usernameVariable: 'MINIO_USERNAME', passwordVariable: 'MINIO_PASSWORD')
            ]){
                docker.withRegistry('https://ghcr.io', registryCredential) {
                    customImage = docker.build('$imagename:dataset-$commit', '--target dataset-image --build-arg MINIO_USERNAME=$MINIO_USERNAME --build-arg MINIO_PASSWORD=$MINIO_PASSWORD .')
                    customImage.push()
            }
          }
        }
      }
    }

    stage('STAGING remove image') {
      when {branch "main"}
        steps {
            sh 'docker rmi $imagename:alembic-$commit'
            sh 'docker rmi $imagename:dataset-$commit'
        }
    }

    stage('STAGING deploy') {
      when {branch "main"}
      steps{
          build(job: "/VRE-IaC/Staging-UpdateAppVersion", parameters: [
            [$class: 'StringParameterValue', name: 'TF_TARGET_ENV', value: 'staging' ],
            [$class: 'StringParameterValue', name: 'TARGET_RELEASE', value: 'dataset' ],
            [$class: 'StringParameterValue', name: 'NEW_APP_VERSION', value: "$commit" ]
        ])
      }
    }
  }
  post {
    failure {
        slackSend color: '#FF0000', message: "Build Failed! - ${env.JOB_NAME} $commit  (<${env.BUILD_URL}|Open>)", channel: 'jenkins-dev-staging-monitor'
    }
  }

}
