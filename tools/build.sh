#!/bin/bash

set -e

VERSION=$(bash tools/version.sh)
DOCKERBASE="suanpan-python-sdk"
if [[ $1 == "master" ]]; then
    echo "build from suanpan-python-sdk:3.7"
    echo "build suanpan-web-process-mining:latest and ${VERSION}"
    TAGS=("3.7")
    BUILD_VERSIONS=(${VERSION})
    BUILD_TAGS=("latest")
else
    echo "build from suanpan-python-sdk:preview-3.7"
    echo "build suanpan-web-process-mining:preview and preview-${VERSION}"
    TAGS=("preview-3.7")
    BUILD_VERSIONS=("preview-${VERSION}")
    BUILD_TAGS=("preview")
fi
BUILDNAMES=("suanpan-web-process-mining")
REQUIREMENTS=("requirements.txt")
NAMESPACE="shuzhi-amd64"
for ((i = 0; i < ${#TAGS[@]}; i++)); do
    for ((j = 0; j < ${#BUILDNAMES[@]}; j++)); do
        docker build --build-arg NAME_SPACE=${NAMESPACE} --build-arg DOCKER_BASE=${DOCKERBASE} \
            --build-arg PYTHON_VERSION=${TAGS[i]} --build-arg REQUIREMENTS_FILE=${REQUIREMENTS[j]} -t \
            registry-vpc.cn-shanghai.aliyuncs.com/${NAMESPACE}/${BUILDNAMES[j]}:${BUILD_VERSIONS[i]} \
            -f docker/Dockerfile .
        docker push registry-vpc.cn-shanghai.aliyuncs.com/${NAMESPACE}/${BUILDNAMES[j]}:${BUILD_VERSIONS[i]}

        docker tag registry-vpc.cn-shanghai.aliyuncs.com/${NAMESPACE}/${BUILDNAMES[j]}:${BUILD_VERSIONS[i]} \
            registry-vpc.cn-shanghai.aliyuncs.com/${NAMESPACE}/${BUILDNAMES[j]}:${BUILD_TAGS[i]}
        docker push registry-vpc.cn-shanghai.aliyuncs.com/${NAMESPACE}/${BUILDNAMES[j]}:${BUILD_TAGS[i]}
    done
done
