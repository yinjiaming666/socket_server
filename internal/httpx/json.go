package httpx

import (
	"encoding/json"
	"net/http"
)

const maxJSONBodyBytes int64 = 1 << 20

// DecodeJSON 解码最多 1 MiB 的请求体并拒绝未知字段。
func DecodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// Response 表示统一 HTTP JSON 响应结构。
type Response struct {
	// Code 表示业务状态码，0 表示成功。
	Code int `json:"code"`
	// Message 表示业务提示信息。
	Message string `json:"message"`
	// Data 表示响应数据。
	Data any `json:"data,omitempty"`
}

// WriteJSON 写入 JSON 响应。
func WriteJSON(w http.ResponseWriter, statusCode int, resp Response) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(resp)
}

// WriteError 写入错误 JSON 响应。
func WriteError(w http.ResponseWriter, statusCode int, message string) {
	WriteJSON(w, statusCode, Response{Code: statusCode, Message: message})
}

// WriteOK 写入成功 JSON 响应。
func WriteOK(w http.ResponseWriter, data any) {
	WriteJSON(w, http.StatusOK, Response{Code: 0, Message: "ok", Data: data})
}
