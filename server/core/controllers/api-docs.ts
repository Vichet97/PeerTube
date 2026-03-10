import { join } from 'path'
import { readFile } from 'fs/promises'
import express from 'express'
import yaml from 'js-yaml'
import { WEBSERVER } from '../initializers/constants.js'
import { root } from '@peertube/peertube-node-utils'
import { apiRateLimiter } from '../middlewares/index.js'

const apiDocsRouter = express.Router()

const SWAGGER_UI_HTML = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PeerTube API - Interactive Documentation</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js"></script>
  <script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-standalone-preset.js"></script>
  <script>
    window.onload = function() {
      const ui = SwaggerUIBundle({
        url: "/api/v1/openapi.json",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIStandalonePreset
        ],
        plugins: [
          SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout",
        persistAuthorization: true
      });
      window.ui = ui;
    };
  </script>
</body>
</html>
`

async function getOpenAPISpec (): Promise<object> {
  const openApiPath = join(root(), 'support', 'doc', 'api', 'openapi.yaml')
  const content = await readFile(openApiPath, 'utf-8')
  const spec = yaml.load(content) as any

  if (spec?.servers && Array.isArray(spec.servers)) {
    spec.servers = [
      { url: WEBSERVER.URL, description: 'This instance' },
      ...spec.servers
    ]
  }

  if (spec?.components?.securitySchemes) {
    spec.components.securitySchemes = {
      ...spec.components.securitySchemes,
      BearerAuth: {
        type: 'http',
        scheme: 'bearer',
        bearerFormat: 'API Token',
        description: 'Paste your API token (from My Account → API tokens) or OAuth access token. Example: peertube_api_xxxxxxxx'
      }
    }
  }

  const httpMethods = [ 'get', 'post', 'put', 'patch', 'delete', 'head', 'options' ]
  if (spec?.paths) {
    for (const pathItem of Object.values(spec.paths) as any[]) {
      if (!pathItem || typeof pathItem !== 'object') continue
      for (const method of httpMethods) {
        const operation = pathItem[method]
        if (operation && Array.isArray(operation.security)) {
          operation.security = [
            { BearerAuth: [] },
            ...operation.security
          ]
        }
      }
    }
  }

  return spec
}

apiDocsRouter.get(
  '/',
  apiRateLimiter,
  (_req: express.Request, res: express.Response) => {
    res.setHeader('Content-Type', 'text/html; charset=utf-8')
    return res.send(SWAGGER_UI_HTML).end()
  }
)

export {
  apiDocsRouter,
  getOpenAPISpec
}
