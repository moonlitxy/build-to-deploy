FROM {{.BaseImage}}
LABEL build-to-deploy.app="{{.App}}" build-to-deploy.version="{{.Tag}}"
ENV TZ=Asia/Shanghai
WORKDIR /app
COPY app /app/app
RUN chmod +x /app/app
{{- if .Port}}
EXPOSE {{.Port}}
{{- end}}
ENTRYPOINT ["/app/app"]
