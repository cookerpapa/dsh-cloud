import { loadCubeApiAuthorizerConfig } from './config.js'
import { createCubeApiAuthorizerServer } from './server.js'

const config = await loadCubeApiAuthorizerConfig()
const server = createCubeApiAuthorizerServer(config.credential)
server.listen(config.port, config.host, () => process.stdout.write('DSH Cloud Cube API authorizer ready\n'))

let closing = false
const close = (): void => {
  if (closing) return
  closing = true
  server.close(error => {
    if (error !== undefined) {
      process.stderr.write('DSH Cloud Cube API authorizer shutdown failed\n')
      process.exitCode = 1
    }
  })
}
process.once('SIGTERM', close)
process.once('SIGINT', close)
