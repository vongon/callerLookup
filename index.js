var fs = require('fs');
var parse = require('csv-parse');
var async = require('async');
var request = require('request');

var secret = require('./secret');

var INPUT_FILE_PATH = './data_test.csv';
var OUTPUT_FILE_NAME = 'out.json';

var main = (cb) => {
  var outputStream = fs.createWriteStream(OUTPUT_FILE_NAME);
  var lineCounter = 0;
  outputStream.write('', (err) => {
    if (err) return cb(err);
    var parser = parse({delimiter: ','}, function (err, data) {
      async.eachSeries(data, (line, sCb) => {
        console.log(`line ${lineCounter++}`);
        if (line[0] == 'Voter File VANID') {
          // skip first line of column headers
          return setImmediate(sCb); 
        }
        processLine(outputStream, line, sCb);
      }, (err) => {
        if (err) return cb(err);
        outputStream.write(']', (err) => {
          if (err) return cb(err);
          outputStream.end();
          return cb();
        });
      });
    });
    fs.createReadStream(INPUT_FILE_PATH).pipe(parser);
  });
}


var processLine = (stream, line, cb) => {
  var vanid = line[0];
  var preferredPhone = line[1];
  var cellPhone = line[2];
  var homePhone = line[3];
  var workPhone = line[4];
  var workExt = line[5];

  var errorCodes = {
    preferredPhone: null,
    cellPhone: null,
    homePhone: null,
    workPhone: null
  };

  async.series([
    (sCb) => {
      // lookup preferred phone
      if (preferredPhone) {
        twilioLookup(preferredPhone, (err, body) => {
          if (err) return sCb(err);
          errorCodes.preferredPhone = body.carrier.error_code;
          return sCb()
        });
      } else {
        return sCb()
      }
    },
    (sCb) => {
      // lookup cell phone
      if (cellPhone) {
        twilioLookup(cellPhone, (err, body) => {
          if (err) return sCb(err);
          errorCodes.cellPhone = body.carrier.error_code;
          return sCb();
        });
      } else {
        return sCb();
      }
    },
    (sCb) => {
      // lookup home phone
      if (homePhone) {
        twilioLookup(homePhone, (err, body) => {
          if (err) return sCb(err);
          errorCodes.homePhone = body.carrier.error_code;
          return sCb();
        });
      } else {
        return sCb();
      }
    },
    (sCb) => {
      // lookup work phone
      if (workPhone) {
        twilioLookup(workPhone, (err, body) => {
          if (err) return sCb(err);
          errorCodes.workPhone = body.carrier.error_code;
          return sCb();
        });
      } else {
        return sCb();
      }
    }
  ], (err) => {
    if(err) return cb(err);
    var outputLine = {
      'Voter File VANID': vanid,
      'Preferred Phone': preferredPhone,
      'Cell Phone': cellPhone,
      'Home Phone': homePhone,
      'WorkPhone': workPhone,
      'WorkPhoneExt': workExt,
      'Preferred Phone Error': errorCodes.preferredPhone || null,
      'Cell Phone Error': errorCodes.cellPhone || null,
      'Home Phone Error': errorCodes.homePhone || null,
      'Work Phone Error': errorCodes.workPhone || null
    };
    stream.write(`${JSON.stringify(outputLine)},\n`, cb); 
  });
}

var twilioLookup = (phoneNumber, cb) => {
  var url = (number) => `https://lookups.twilio.com/v1/PhoneNumbers/${number}?CountryCode=US&Type=carrier`
  var options = {
    auth: {
      user: secret.user,
      pass: secret.pass
    }
  };
  request(url(phoneNumber), options, (err, res, body) => {
    if (err) return cb(err);
    if (res.statusCode != 200) {
      return cb(res);
    }
    return cb(null, JSON.parse(body));
  });
}


main((err) => {
  if (err) {
    throw err;
  }
  console.log('Done!');
});