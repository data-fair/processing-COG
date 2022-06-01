const fs = require('fs-extra')
const util = require('util')
const pump = util.promisify(require('pump'))
const dayjs = require('dayjs')
const FormData = require('form-data')
const path = require('path')

function displayBytes (aSize) {
  aSize = Math.abs(parseInt(aSize, 10))
  if (aSize === 0) return '0 octets'
  const def = [[1, 'octets'], [1000, 'ko'], [1000 * 1000, 'Mo'], [1000 * 1000 * 1000, 'Go'], [1000 * 1000 * 1000 * 1000, 'To'], [1000 * 1000 * 1000 * 1000 * 1000, 'Po']]
  for (let i = 0; i < def.length; i++) {
    if (aSize < def[i][0]) return (aSize / def[i - 1][0]).toLocaleString() + ' ' + def[i - 1][1]
  }
}

const withStreamableFile = async (filePath, fn) => {
  // creating empty file before streaming seems to fix some weird bugs with NFS
  await fs.ensureFile(filePath + '.tmp')
  await fn(fs.createWriteStream(filePath + '.tmp'))
  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  // const fd = await fs.open(filePath + '.tmp', 'r')
  // await fs.fsync(fd)
  // await fs.close(fd)
  // write in tmp file then move it for a safer operation that doesn't create partial files
  await fs.move(filePath + '.tmp', filePath, { overwrite: true })
}

exports.download = async (dir = 'data', axios, log) => {
  const urlINSEE = 'https://www.insee.fr/fr/statistiques/fichier/6051727/pays_' + dayjs().year() + '.csv'
  // const urlStable = ' https://www.data.gouv.fr/fr/datasets/r/0818ebbd-8f45-435a-a074-1aec5b14d47c'
  const filename = 'COG_' + dayjs().year() + '.csv'
  await log.step(`Téléchargement de ${filename}`)
  const filePath = `${dir}/${filename}`
  await withStreamableFile(filePath, async (writeStream) => {
    const res = await axios({ url: urlINSEE, method: 'GET', responseType: 'stream' })
    await pump(res.data, writeStream)
  })
}

exports.upload = async (processingConfig, tmpDir, axios, log, patchConfig) => {
  const datasetSchema = require('./schema.json')
  const formData = new FormData()

  if (processingConfig.datasetMode === 'update') {
    await log.step('Mise à jour du jeu de données')
  } else {
    formData.append('schema', JSON.stringify(datasetSchema))
    formData.append('title', processingConfig.dataset.title)
    await log.step('Création du jeu de données')
  }

  const filePath = path.join(tmpDir, 'COG_2022.csv')
  formData.append('dataset', fs.createReadStream(filePath), { filename: 'COG_2022.csv' })
  formData.getLength = util.promisify(formData.getLength)
  const contentLength = await formData.getLength()
  await log.info(`chargement de ${displayBytes(contentLength)}`)
  const dataset = (await axios({
    method: 'post',
    url: (processingConfig.dataset && processingConfig.dataset.id) ? `api/v1/datasets/${processingConfig.dataset.id}` : 'api/v1/datasets',
    data: formData,
    maxContentLength: Infinity,
    maxBodyLength: Infinity,
    headers: { ...formData.getHeaders(), 'content-length': contentLength }
  })).data

  if (processingConfig.datasetMode === 'update') {
    await log.info(`jeu de donnée mis à jour, id="${dataset.id}", title="${dataset.title}"`)
  } else {
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
  }
}
