pipeline {
    agent { label 'small' }
    environment {
      imagename = "ghcr.io/pilotdataplatform/dataset"
      commit = sh(returnStdout: true, script: 'git describe --always').trim()
      registryCredential = 'pilot-ghcr'
      dockerImage = ''
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

    // stage('DEV unit test') {
    //   when {branch "develop"}
    //   steps{
    //     withCredentials([
    //       string(credentialsId:'VAULT_TOKEN', variable: 'VAULT_TOKEN'),
    //       string(credentialsId:'VAULT_URL', variable: 'VAULT_URL'),
    //       file(credentialsId:'VAULT_CRT', variable: 'VAULT_CRT')
    //     ]) {
    //       sh """
    //       export REDIS_HOST=127.0.0.1
    //       pip install --user poetry==1.1.12
    //       ${HOME}/.local/bin/poetry config virtualenvs.in-project true
    //       ${HOME}/.local/bin/poetry install --no-root --no-interaction
    //       ${HOME}/.local/bin/poetry run pytest --verbose -c tests/pytest.ini
    //       """
    //     }
    //   }
    // }

    stage('DEV build and push image') {
      when {branch "develop"}
      steps {
        script {
           {
            docker.withRegistry('https://ghcr.io', registryCredential) {
                customImage = docker.build("$imagename:$commit", ".")
                customImage.push()
            }
          }
        }
      }
    }

    stage('DEV remove image') {
      when {branch "develop"}
      steps{
        sh "docker rmi $imagename:$commit"
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
            withCredentials([
                usernamePassword(credentialsId:'minio', usernameVariable: 'MINIO_USERNAME', passwordVariable: 'MINIO_PASSWORD')
          ]){
            docker.withRegistry('https://ghcr.io', registryCredential) {
                customImage = docker.build("$imagename:$commit", "--build-arg MINIO_USERNAME=$MINIO_USERNAME --build-arg MINIO_PASSWORD=$MINIO_PASSWORD .")
                customImage.push()
            }
          }
        }
      }
    }

    stage('STAGING remove image') {
      when {branch "main"}
      steps{
        sh "docker rmi $imagename:$commit"
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
