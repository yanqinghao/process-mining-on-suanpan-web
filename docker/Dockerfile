ARG PYTHON_VERSION

ARG NAME_SPACE

ARG DOCKER_BASE

FROM registry-vpc.cn-shanghai.aliyuncs.com/${NAME_SPACE}/${DOCKER_BASE}:${PYTHON_VERSION}

WORKDIR /workspace

COPY . /workspace

# ENV PYPI_MIRROR "https://mirrors.aliyun.com/pypi/simple"

# RUN pip config set global.index-url ${PYPI_MIRROR}

# RUN pip install --upgrade pip

# RUN pip install -r requirements.txt

ENTRYPOINT [ "/sbin/my_init", "--" ]

CMD [ "bash" ]
