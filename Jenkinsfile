pipeline {
    agent { label 'small' }
    environment {
      imagename = "ghcr.io/pilotdataplatform/metadata"
      commit = sh(returnStdout: true, script: 'git describe --always').trim()
      registryCredential = 'pilot-ghcr'
      dockerImage = ''
    }

    stages {

    stage('Git clone for dev') {
        when {branch "develop"}
        steps{
          script {
          git branch: "develop",
              url: 'https://github.com/PilotDataPlatform/dataset.git',
              credentialsId: 'pilot-ghcr'
            }
        }
    }

    stage('DEV unit test') {
      when {branch "develop"}
      steps{
        withCredentials([
          string(credentialsId:'VAULT_TOKEN', variable: 'VAULT_TOKEN'),
          string(credentialsId:'VAULT_URL', variable: 'VAULT_URL'),
          file(credentialsId:'VAULT_CRT', variable: 'VAULT_CRT')
        ]) {
          sh """
          export REDIS_HOST=127.0.0.1
          export VAULT_TOKEN=${VAULT_TOKEN}
          export VAULT_URL=${VAULT_URL}
          export VAULT_CRT=${VAULT_CRT}
          export ROOT_PATH=/data/vre-storage
          export ROOT_PATH="/data/vre-storage"
          pip install --user poetry==1.1.12
          ${HOME}/.local/bin/poetry config virtualenvs.in-project true
          ${HOME}/.local/bin/poetry install --no-root --no-interaction
          ${HOME}/.local/bin/poetry run pytest --verbose -c tests/pytest.ini
          """
        }
      }
    }

    stage('DEV Build and push image') {
      when {branch "develop"}
      steps{
        script {
          withCredentials([
            usernamePassword(credentialsId:'readonly', usernameVariable: 'PIP_USERNAME', passwordVariable: 'PIP_PASSWORD'),
            usernamePassword(credentialsId:'minio', usernameVariable: 'MINIO_USERNAME', passwordVariable: 'MINIO_PASSWORD')
          ]) {
            docker.withRegistry('https://registry-gitlab.indocresearch.org', registryCredential) {
                customImage = docker.build("registry-gitlab.indocresearch.org/pilot/service_dataset:${commit}",  "--build-arg MINIO_USERNAME=$MINIO_USERNAME --build-arg MINIO_PASSWORD=$MINIO_PASSWORD --build-arg pip_username=${PIP_USERNAME} --build-arg pip_password=${PIP_PASSWORD} --add-host git.indocresearch.org:10.4.3.151 .")
                customImage.push()
            }
          }
        }
      }
    }
    stage('DEV Remove image') {
      when {branch "develop"}
      steps{
        sh "docker rmi $imagename_dev:$commit"
      }
    }

    stage('DEV Deploy') {
      when {branch "develop"}
      steps{
      build(job: "/VRE-IaC/UpdateAppVersion", parameters: [
        [$class: 'StringParameterValue', name: 'TF_TARGET_ENV', value: 'dev' ],
        [$class: 'StringParameterValue', name: 'TARGET_RELEASE', value: 'dataset' ],
        [$class: 'StringParameterValue', name: 'NEW_APP_VERSION', value: "$commit" ]
    ])
      }
    }

    stage('Git clone staging') {
        when {branch "main"}
        steps{
          script {
          git branch: "main",
              url: 'https://github.com/PilotDataPlatform/dataset.git',
              credentialsId: 'pilot-ghcr'
            }
        }
    }

    stage('STAGING Building and push image') {
      when {branch "main"}
      steps{
        script {
          withCredentials([
            usernamePassword(credentialsId:'readonly', usernameVariable: 'PIP_USERNAME', passwordVariable: 'PIP_PASSWORD'),
            usernamePassword(credentialsId:'minio', usernameVariable: 'MINIO_USERNAME', passwordVariable: 'MINIO_PASSWORD')
          ]) {
            docker.withRegistry('https://registry-gitlab.indocresearch.org', registryCredential) {
                customImage = docker.build("registry-gitlab.indocresearch.org/pilot/service_dataset:${commit}",  "--build-arg MINIO_USERNAME=$MINIO_USERNAME --build-arg MINIO_PASSWORD=$MINIO_PASSWORD --build-arg pip_username=${PIP_USERNAME} --build-arg pip_password=${PIP_PASSWORD} --add-host git.indocresearch.org:10.4.3.151 .")
                customImage.push()
            }
          }
        }
      }
    }

    stage('STAGING Remove image') {
      when {branch "main"}
      steps{
        sh "docker rmi $imagename_staging:$commit"
      }
    }

    stage('STAGING Deploy') {
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
