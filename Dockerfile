FROM debian:stable-slim AS build_image
WORKDIR build

ADD pom.xml pom.xml
ADD server server
ADD build.images.sh build.images.sh

RUN bash ./build.images.sh

FROM debian:stable-slim
WORKDIR dynamo
COPY --from=build_image /build/server/server-1.0  /dynamo/server
COPY ./execute.sh /dynamo/execute.sh

RUN chmod -R 777 /dynamo

EXPOSE 8000/tcp

CMD ["sh", "/dynamo/execute.sh"]


