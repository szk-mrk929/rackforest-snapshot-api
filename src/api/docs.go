package api

import (
	"net/http"

	swaggerui "github.com/alexliesenfeld/go-swagger-ui"
)

// mountDocs serves Swagger UI at /docs from the OpenAPI document embedded
// by code generation. That document is the api/openapi.yaml spec.
func mountDocs(mux *http.ServeMux) {
	opts := []swaggerui.Option{
		swaggerui.WithHTMLTitle("RackForest Snapshot API"),
		swaggerui.WithBasePath("/docs"),
		swaggerui.WithDocExpansion(swaggerui.DocExpansionList),
		swaggerui.WithDeepLinking(true),
		swaggerui.WithTryItOutEnabled(true),
		swaggerui.WithShowCommonExtensions(true),
	}
	if spec, err := GetSpecJSON(); err == nil {
		opts = append(opts, swaggerui.WithSpec(spec))
	}
	ui := swaggerui.NewHandler(opts...)
	mux.HandleFunc("GET /docs", ui)
	mux.HandleFunc("GET /docs/", ui)
	mux.HandleFunc("GET /docs/{file}", ui)
	mux.HandleFunc("GET /docs/openapi.json", serveOpenAPI)
}

func serveOpenAPI(w http.ResponseWriter, _ *http.Request) {
	spec, err := GetSpecJSON()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(spec)
}
