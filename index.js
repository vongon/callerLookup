var fs = require('fs');
var parse = require('csv-parse');
var async = require('async');
var request = require('request');

var secret = require('./secret');

var INPUT_FILE_PATH = './data_test.csv';
var OUTPUT_FILE_NAME = 'out.txt';

var main = (cb) => {
	var outputStream = fs.createWriteStream(OUTPUT_FILE_NAME);
	var parser = parse({delimiter: ','}, function (err, data) {
	  async.eachSeries(data, (line, sCb) => {
	    if (line[0] == 'Voter File VANID') {
	    	// skip first line of column headers
	    	return setImmediate(sCb);	
	    }
	    processLine(outputStream, line, sCb);
	  }, (err) => {
			if (err) return cb(err);
	  	outputStream.end();
	  	return cb();
	  });
	});
	fs.createReadStream(INPUT_FILE_PATH).pipe(parser);
}


var processLine = (stream, line, cb) => {
	var vanid = line[0];
	var preferredPhone = line[1];
	var cellPhone = line[2];
	var homePhone = line[3];
	var workPhone = line[4];
	var workExt = line[5];
	twilioLookup(preferredPhone, (err) => {
		if (err) return cb(err);
		stream.write(`${JSON.stringify({vanid, preferredPhone})}\n`, cb);	
	});
	
}

var twilioLookup = (phoneNumber, cb) => {
	var url = (number) => `https://lookups.twilio.com/v1/PhoneNumbers/${number}?CountryCode=US&Type=carrier`
	var options = {
		auth: {
			user: secret.user,
			pass: secret.pass
		}
	}
	request(url(phoneNumber), options, (err, res, body) => {
		if (err) return cb(err);
		if (res.statusCode != 200) {
			return cb(res);
		}
		console.log({body});
		return cb();
	})
}


main((err) => {
	if (err) {
		throw err;
	}
	console.log('Done!');
});