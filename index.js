
const createBulkWriteStream = require('bulk-insert')
const moment = require('moment')
const AWS = require('aws-sdk')
const { v4 } = require('uuid')
const http = require('http')
const url = require('url')

AWS.config.update({region:'ap-south-1'});

const GIF = new Buffer('R0lGODlhAQABAAAAACH5BAEAAAAALAAAAAABAAEAAAI=', 'base64')
const stream = new AWS.Firehose()
const DeliveryStreamName = 'mx-ads-event-tracker'

function onError (err) {
  if (err) 
  console.error(err.stack);
};

const writer = createBulkWriteStream({
  interval: 300, // every 300ms
  max: 500, // 500 records at a time
  onError,
  flush(data) {
    stream.putRecordBatch({
      Records: data.map((x) => ({
        Data: `${JSON.stringify(x)}`
      })),
      DeliveryStreamName
    }, onError)
  }
});

http.createServer().on('request', (req, res) => {
  const { query } = url.parse(req.url, true)
  query.id = v4()
  query.received_at = moment.utc().format('YYYY-MM-DD HH:mm:ss')
  writer.write(query)
  
  res.setHeader('Cache-Control', 'private, no-cache, no-store, max-age=0')
  res.setHeader('Content-Length', GIF.length)
  res.setHeader('Content-Type', 'image/gif')
  res.end(GIF)
}).listen(process.env.PORT || 3000, function (err) {
  if (err) throw err
  console.log('tracking server listening on port %s', this.address().port)
});