const r = require('.')
const { download, upload } = require('./src/steps')

exports.run = async ({ processingConfig, tmpDir, axios, log, patchConfig }) => {
  await download(tmpDir, axios, log)
  if (!processingConfig.skipUpload) await upload(processingConfig, tmpDir, axios, log, patchConfig)
}
r.run({})
