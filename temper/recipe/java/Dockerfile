# River's Java port, built from this checkout, plus the scenario programs.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY java/ java/
RUN cd java && mvn -q -B -DskipTests -pl river -am install \
 && mvn -q -B -pl river dependency:copy-dependencies -DoutputDirectory=/deps -DincludeScope=runtime \
 && cp "$(ls river/target/river-*.jar | grep -v -e sources -e javadoc)" /river.jar

FROM eclipse-temurin:21
COPY --from=build /river.jar /river.jar
COPY --from=build /deps /deps
COPY temper/recipe/java/*.java /recipe/
ENV JAVA_TOOL_OPTIONS=--enable-native-access=ALL-UNNAMED
