FROM lambci/lambda:build-python3.7

COPY requirements.txt requirements.txt

# Install and compress libraries
# See https://github.com/ryfeus/lambda-packs/blob/master/Pandas_numpy/buildPack.sh
RUN pip install -r requirements.txt -t target/package && \
  cd target/package && \
  find . -type d -name "tests" -exec rm -rf {} + && \
  find . -name "*.so" | xargs strip && \
  find . -name "*.so.*" | xargs strip

COPY src/main/python3/ target/package
COPY src/main/sql/ target/package

ARG PROJECT_NAME
ENV PACKAGE_NAME lambda-${PROJECT_NAME}.zip
WORKDIR target/package
RUN zip -q -x "*__pycache__*" -r9 ../${PACKAGE_NAME} .
WORKDIR ../..

VOLUME target/host
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["cp target/${PACKAGE_NAME} target/host/"]
