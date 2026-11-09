FROM {{.BaseImage}}
LABEL build-to-deploy.app="{{.App}}" build-to-deploy.version="{{.Tag}}"
ENV TZ=Asia/Shanghai
ENV JAVA_OPTS="{{.JavaOpts}}"
WORKDIR /app
COPY app.jar /app/app.jar
{{- if .Port}}
EXPOSE {{.Port}}
{{- end}}
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
