package api

import (
	"net"
	"net/http"

	"build-to-deploy/internal/pipeline"
)

// clientIP 取请求来源地址（本服务只允许本机访问，通常是 127.0.0.1 或 ::1）
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// listYamlBackups 列出本机保存的远程 YAML 快照，不访问目标服务器
func (s *Server) listYamlBackups(w http.ResponseWriter, r *http.Request) {
	list, err := pipeline.ListYamlBackups(s.cfg)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

// downloadYamlBackup 下载指定应用的某份快照；文件名只允许纯文件名，拒绝路径穿越
func (s *Server) downloadYamlBackup(w http.ResponseWriter, r *http.Request) {
	appID := r.URL.Query().Get("appId")
	name := r.URL.Query().Get("name")
	_, content, err := pipeline.ReadYamlBackup(s.cfg, appID, name)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/x-yaml; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+name+"\"")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write([]byte(content))
}

// restoreYamlBackup 恢复快照：apply=false 只写回文件；apply=true 写回后执行部署命令并等待就绪
func (s *Server) restoreYamlBackup(w http.ResponseWriter, r *http.Request) {
	if !s.requireConn(w) {
		return
	}
	var req struct {
		AppID string `json:"appId"`
		Name  string `json:"name"`
		Apply bool   `json:"apply"`
	}
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	rel, err := s.engine.StartYamlRestore(pipeline.YamlRestoreRequest{
		AppID:    req.AppID,
		Name:     req.Name,
		Apply:    req.Apply,
		ClientIP: clientIP(r),
	})
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, rel)
}
