const { PassThrough, Writable, Readable, Transform } = require('stream');
var fs = require('fs');
var path = require('path');
const parse = require('csv-parse');
var stringify = require('csv-stringify');
const pass = new PassThrough();
const writable = new Writable();

// const csvFile = 'fs_read.csv';
const csvFile = 'easy.csv';
// const csvFile = '../../Desktop/Datasets/OCOD_COU_2018_05.csv';
// const csvFile = '../../Desktop/Datasets/CCOD_FULL_2018_05.csv';

const oneIn = 100;              //  log only a sample of records - one in every oneIn
const decodingOptions = {
  delimiter: ',',
  trim:true,
  relax_column_count: true
}
const encodingOptions = {
  delimiter: ','
}

var count=0;
var bytes=0;
const writeables = [];
const filenames = [];
var headRow;
const headRowNumber = 0;

var parser = parse (decodingOptions);
const stringifier = stringify(encodingOptions);

const log = (chunk, count, message) => {
  const type = Array.isArray(chunk)? 'Array: ' : typeof chunk;

  if (Math.floor(count/100)%oneIn ==0)
    console.log(message, type, chunk, count);
}


const existsPrefix = prefix => {
  return !!filenames [prefix]
}

const prefixOf = (row, count) => {
  if (count===0)                          // Assume first row is headers (but allow for count being undefined)
    return ('HEADER');
  testChunk = Buffer.isBuffer(row)?       // Accept buffer, array with string at [0], or get confused
    row.slice(0,8).toString()
  : Array.isArray(headRow) && (typeof row[0] == 'string') ?
    row[0]
    : 'conversion_confusion'

  const regEx = /^([a-zA-Z]*)(\d+)$/ ;

  var prefix = testChunk.match (regEx) || [,'special'];    // Did it match regex? eg Z12345Z would not
  prefix = prefix[1];

  return prefix
}

//////////////// Transformers /////////////////////////////

const discriminator = reqdPrefix => new Transform({
  transform(chunk, encoding, callback) {
    if (prefixOf (chunk) === reqdPrefix)          // very inefficient - you should pass or store prefix
      this.push(chunk);
    callback();
  }
});


const addNewWriteable = prefix => {
  const filename = `./summat${prefix}.csv`;
  writeables[prefix] = fs.createWriteStream(filename, { flags : 'w' });
  console.log('_____________________________');
  if (headRow)                                                        // csv-stringify Mixed API
    stringify (headRow, encodingOptions,
      (_,data) => writeables[prefix].write(data));   // .write(str(headRow)) gets called AFTER all pipes :(
  data
    .pipe(discriminator(prefix))
    .pipe(writeables[prefix]);

  console.log('Made new writeable for ',prefix);
  filenames[prefix]=filename;
  return writeables[prefix]
}

//////////////// event listeners /////////////////////////////

parser.on('data', (chunk) => {
  bytes += chunk.length;
  const prefix = prefixOf(chunk);

  // console.log('Prefix of [',chunk[0],']etc. =',prefix);

  if (!headRow && count==headRowNumber)    // then don't make a new writeable for this line, just set headRow
    headRow = chunk;
  else
    if (!existsPrefix (prefix))
      addNewWriteable (prefix);

  log (chunk, count, 'Parser got:');
  count++;
});


stringifier.on('data', (chunk) => {         // should remain unecessary
  log (chunk, count, 'Stringifier got:');
  // chunk = chunk.toString();
});


parser.on('end', () => {
   console.log(`After ${count} records, done`);
   writeables.forEach (writeable => writeable.end());
});

parser.on ('error', err => {
   console.log(`After ${bytes} bytes,`);
   console.log(err);
});


/////// Do it! - Everything's piped, so once we read, we transform and write ///////

var data =fs.createReadStream (path.join (__dirname, csvFile))
  .pipe(parser)                 // outputs arrays
  .pipe(stringifier)            // outputs buffers
