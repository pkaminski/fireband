'use strict';

let gcloud = require('gcloud');
let ms = require('ms');

const PROTOCOL_PATH = '/$protocol';
const DEBUG_PATH = '/$debug';
const TIME_OFFSET_REGEX = /^event: \/\.info\/serverTimeOffset:value:(-?\d+)/;
const LOG_REGEX = /^p:\d+: ([^{]*)(\{.*)/;
const READ_REGEX = /^(from server:|handleServerMessage) (\w*)/;
const READ_PATH_REGEX = /^\{"p":"([^"]*)"/;
const WRITE_REGEX = /^\{"r":\d+,"a":"(\w+)","b":\{("p":"([^"]+))?"/;

let lastFlushTimestamp = 0;
let serverTimeOffset = 0;

function BandwidthCollector(gcloud, options) {
  this._bigquery = gcloud.bigquery();
  this._options = options;
  this._takeData();
  setInterval(this.flushIfLate.bind(this), options.forcedFlushInterval);
  this.collectLog = this._collectLog.bind(this);
}

BandwidthCollector.prototype._collectLog = function(message) {
  try {
    let match = message.match(LOG_REGEX);
    if (!match) {
      match = message.match(TIME_OFFSET_REGEX);
      if (match) serverTimeOffset = parseInt(match[1], 10);
      return;
    }
    let prefix = match[1], value = match[2];
    let row = prefix ? this._parseReadMessage(prefix, value) : this._parseWriteMessage(value);
    if (!row) return;
    row.time = (Date.now() + serverTimeOffset) / 1000;
    if (this._options.tag) row.tag = this._options.tag;
    this._pushRow(row);
  } catch (e) {
    console.log('Unexpected error while collecting Firebase bandwidth:', e.stack);
  }
};

BandwidthCollector.prototype._parseReadMessage = function(prefix, value) {
  let details = prefix.match(READ_REGEX);
  if (!details) return;  // not interested in this message, it's redundant
  let messageType = details[1] === 'from server: ' ? '' : details[2];
  let row = {op: 'r', size: value.length + 16 + (messageType ? 7 + messageType.length : 0)};
  switch (messageType) {
    case 'd':  // data
    case 'm':  // child moved
      {
        let pathDetails = value.match(READ_PATH_REGEX);
        if (!pathDetails) {
          console.log('No path found in message:', prefix, value);
          return;
        }
        row.path = '/' + pathDetails[1];
        break;
      }
    case '':  // request response
      row.path = PROTOCOL_PATH;
      break;
    case 'sd':  // debug info
      row.path = DEBUG_PATH;
      break;
    default:
      console.log('Unknown write type:', messageType);
      return;
  }
  return row;
};

BandwidthCollector.prototype._parseWriteMessage = function(value) {
  let details = value.match(WRITE_REGEX);
  let messageType = details[1], path = details[3];
  if (!details) {
    console.log('Failed to match write log:', value);
    return;
  }
  let row = {op: 'w', size: value.length + 16};
  switch (messageType) {
    case 'm':  // update
    case 'p':  // set, remove, transaction
      if (!path) {
        console.log('No path found in message:', value);
        return;
      }
      row.path = path;
      break;
    case 'q':  // listen
    case 'l':  // another kind of listen?
    case 'n':  // unlisten
    case 'auth':  // authentication
    case 's':  // stats
      row.path = PROTOCOL_PATH;
      break;
    default:
      console.log('Unknown write type:', messageType);
      return;
  }
  return row;
};

BandwidthCollector.prototype._pushRow = function(row) {
  this._data.push(row);
  this._dataSize += row.size;
  if (this._dataSize >= this._options.sizeFlushThreshold) this.flush();
};

BandwidthCollector.prototype._takeData = function() {
  let data = this._data;
  this._data = [];
  this._dataSize = 0;
  return data;
};

BandwidthCollector.prototype.flush = function() {
  lastFlushTimestamp = Date.now();
  if (!this._data.length) return;
  let date = new Date();
  let tableName =
    this._options.tablePrefix + date.getUTCFullYear() + (date.getUTCMonth() + 1) +
    date.getUTCDate();
  let table = this._bigquery.dataset(this._options.datasetId).table(tableName);
  let rows = this._takeData();
  console.log('Flushing', rows.length, 'rows of bandwidth data to BigQuery table', tableName);
  table.insert(rows, function(error, insertErrors) {
    // gcloud will already retry intermittent or rate-limit errors, so if we get here just log the
    // error and drop the data.  Re-enqueuing bad data could cause a feedback loop.
    if (error) console.log('Error sending bandwidth data to BigQuery:', error);
    if (insertErrors) console.log('Error sending bandwidth data to BigQuery:', insertErrors);
  });
};

BandwidthCollector.prototype.flushIfLate = function() {
  try {
    if (Date.now() - lastFlushTimestamp >= this._options.forcedFlushInterval) this.flush();
  } catch (e) {
    console.log('Unexpected error while collecting Firebase bandwidth:', e.stack);
  }
};

/**
 * Create a new bandwidth collector that writes sizes of packets sent/received to a BigQuery table.
 * After creating a new collector with the options below, you need to call:
 *   Firebase.enableLogging(collector.collectLog);
 * Options are:
 *   projectId   (required, project name in Google Cloud)
 *   datasetId   (required, dataset name in BigQuery)
 *   tablePrefix (optional, default: 'raw'; will have date appended, tables must already exist)
 *   keyFilename (optional, path to service account key file; either this or key is required)
 *   key         (optional, contents of service account JSON key file if keyFilename not specified)
 *   tag         (optional, default none; helps categorize traffic in BigQuery)
 *   sizeFlushThreshold
 *               (optional, default 100KB; flush rows to BQ immediately after buffering data about
 *               this much traffic)
 *   forcedFlushInterval
 *               (optional, default 1 minute; flush rows to BQ no less often 1x to 2x this interval)
 */
module.exports = function(options) {
  let gcloudOptions = {projectId: options.projectId};
  if (options.keyFilename) {
    gcloudOptions.keyFilename = options.keyFilename;
  } else {
    gcloudOptions.credentials = JSON.parse(options.key);
  }
  return new BandwidthCollector(gcloud(gcloudOptions), {
    datasetId: options.datasetId, tablePrefix: options.tablePrefix || 'raw', tag: options.tag,
    sizeFlushThreshold: options.sizeFlushThreshold || 100000,
    forcedFlushInterval: options.forcedFlushInterval || ms('1m')
  });
};
