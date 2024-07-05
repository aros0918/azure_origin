/*
    MySql Database implementation
*/

const config = require('../config');
const engine = require('mysql2');
const csv = require('csvtojson/v1')
const https = require('https')

const connection = engine.createConnection({
  host     : config.dbHost,
  port     : config.dbPort,
  user     : config.dbUser,
  password : config.dbPassword,
  database : config.dbBase,
});

function handleDisconnect() {
    connection = engine.createConnection({
        host     : config.dbHost,
        port     : config.dbPort,
        user     : config.dbUser,
        password : config.dbPassword,
        database : config.dbBase,
      }); // Recreate the connection, sincethe old one cannot be reused.
  
    connection.connect(function(err) {              // The server is either down
      if(err) {                                     // or restarting (takes a while sometimes).
        console.log('error when connecting to db:', err);
        setTimeout(handleDisconnect, 2000); // We introduce a delay before attempting to reconnect,
      }                                     // to avoid a hot loop, and to allow our node script to
    });                                     // process asynchronous requests in the meantime.
                                            // If you're also serving http, display a 503 error.
    connection.on('error', function(err) {
      console.log('db error', err);
      if(err.code === 'PROTOCOL_CONNECTION_LOST') { // Connection to the MySQL server is usually
        handleDisconnect();                         // lost due to either server restart, or a
      } else {                                      // connnection idle timeout (the wait_timeout
        throw err;                                  // server variable configures this)
      }
    });
  }
  
handleDisconnect();


var database = {};

database.init = function(onConnected){
    connection.connect();
    console.log("Creating/checking CowList");
    var queryText = "CREATE TABLE if not exists CowList (ID INTEGER PRIMARY KEY AUTO_INCREMENT, Eartag VARCHAR(32) NOT NULL, CalvingDate DATE, Feed24hrSup1 INTEGER, Feed24hrSup2 INTEGER,  UNIQUE `idx_eartag` (`Eartag`));";
    //queryText += "CREATE TABLE if not exists CowActivity (ID INTEGER PRIMARY KEY AUTO_INCREMENT, Eartag VARCHAR(32) NOT NULL, VisitTime TIMESTAMP, GramSup1 INTEGER, GramSup2 INTEGER, Comment TEXT, Syncronized INTEGER, SlaveID TEXT, Herd TEXT, Country TEXT, ActivityCode INTEGER);";
    //queryText += "CREATE TABLE if not exists Config (ID INTEGER PRIMARY KEY AUTO_INCREMENT, GramsPerDoseSup1 INTEGER, GramsPerDoseSup2 INTEGER, DosesPerVisitSup1 INTEGER, DosesPerVisitSup2 INTEGER, MaxDosesPer24hr INTEGER, AccumulationDays INTEGER, StartDaySup1 INTEGER, StartDaySup2 INTEGER, EndDaySup1 INTEGER, EndDaySup2 INTEGER, CalibrationMode INTEGER, NameSup1 TEXT, NameSup2 TEXT, DefaultGramsPerDaySup1 INTEGER, DefaultGramsPerDaySup2 INTEGER);";
    connection.query(queryText, function (error, results) {
        if (error) throw error;
        console.log("Creating/checking CowActivity");
        queryText = "CREATE TABLE if not exists CowActivity (ID INTEGER PRIMARY KEY AUTO_INCREMENT, Eartag VARCHAR(32) NOT NULL, VisitTime TIMESTAMP, GramSup1 INTEGER, GramSup2 INTEGER, Comment TEXT, Syncronized INTEGER, SlaveID TEXT, Herd TEXT, Country TEXT, ActivityCode INTEGER);";
        connection.query(queryText, function (error, results) {
            if (error) throw error;
            queryText = "CREATE TABLE if not exists Config (ID INTEGER PRIMARY KEY AUTO_INCREMENT, GramsPerDoseSup1 INTEGER, GramsPerDoseSup2 INTEGER, DosesPerVisitSup1 INTEGER, DosesPerVisitSup2 INTEGER, MaxDosesPer24hr INTEGER, AccumulationDays INTEGER, StartDaySup1 INTEGER, StartDaySup2 INTEGER, EndDaySup1 INTEGER, EndDaySup2 INTEGER, CalibrationMode INTEGER, NameSup1 TEXT, NameSup2 TEXT, DefaultGramsPerDaySup1 INTEGER, DefaultGramsPerDaySup2 INTEGER);";
            connection.query(queryText, function (error, results) {
                if (error) throw error;
                queryText = "INSERT IGNORE INTO Config (ID, GramsPerDoseSup1, GramsPerDoseSup2, DosesPerVisitSup1, DosesPerVisitSup2, MaxDosesPer24hr, AccumulationDays, StartDaySup1, StartDaySup2, EndDaySup1, EndDaySup2, CalibrationMode, NameSup1, NameSup2, DefaultGramsPerDaySup1, DefaultGramsPerDaySup2) VALUES (1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'None', 'None', 0, 0)";
                connection.query(queryText, function (error, results) {
                    if (error) throw error;
                    if (typeof onConnected === "function") {
                        onConnected();
                    }
                });
            });
        });
    });
}

database.readConfigFile = function(onResults, onError){

    https.get(config.paramsUrl, (resp) => {
        var csvData = '';
        resp.on('data', (chunk) => {
            csvData += chunk;
        });
        resp.on('end', () => {
            csv({delimiter:';'}).fromString(csvData)
            .on('end_parsed',(jsonParams)=>{
                if (jsonParams.length == 0) {
                    onError("Error while parsing parameters from "+config.paramsUrl);
                    return;
                }
                var row = jsonParams[0];
                var calibMode = "0";
                if (row['kalibmode']=="True") calibMode = "1";

                var queryText = "UPDATE Config SET ";
                queryText += " GramsPerDoseSup1 = '"+row['supplement1perdose'];
                queryText += "', GramsPerDoseSup2 = '"+row['supplement2perdose'];
                queryText += "', DosesPerVisitSup1 = '"+row['dosespervisit1'];
                queryText += "', DosesPerVisitSup2 = '"+row['dosespervisit2'];
                queryText += "', MaxDosesPer24hr = '"+row['24hoursvisitsmax'];
                queryText += "', AccumulationDays = '"+row['rollingperiod'];
                queryText += "', StartDaySup1 = '"+row['startday1'];
                queryText += "', StartDaySup2 = '"+row['startday2'];
                queryText += "', EndDaySup1 = '"+row['endday1'];
                queryText += "', EndDaySup2 = '"+row['endday2'];
                queryText += "', CalibrationMode = '"+calibMode;
                queryText += "', NameSup1 = '"+row['name1'];
                queryText += "', NameSup2 = '"+row['name2'];
                queryText += "', DefaultGramsPerDaySup1 = '"+row['default1perday'];
                queryText += "', DefaultGramsPerDaySup2 = '"+row['default2perday'];
                queryText += "'";
                connection.execute(queryText, [] ,function (err) {
                    if (err) {
                        console.log(err);
                    } else {
                        onResults();
                    }
                });


            })
            .on('error',(err)=>{
                console.log("CSV : Error",err)
            });
        });
    });
}

database.readCowListFile = function(onResults){

    https.get(config.cowListUrl, (resp) => {
        var csvData = '';
        resp.on('data', (chunk) => {
            csvData += chunk;
        });
        resp.on('end', () => {
            csv({delimiter:';'}).fromString(csvData)
            .on('end_parsed',(jsonList)=>{
                for (var rowNum = 0; rowNum < jsonList.length; rowNum++) {
                    var row = jsonList[rowNum];
                    var queryText = "REPLACE INTO CowList (Eartag, CalvingDate, Feed24hrSup1, Feed24hrSup2) VALUES ('" + row['tagID'] + "', '" + row['calvingdate'] + "', " + row['24hoursallowancesup1']  + ", " + row['24hoursallowancesup2'] + ")";
                    connection.execute(queryText);
                }
            })
            .on('error',(err)=>{
                console.log("CSV : Error",err)
            });
        });
    });
}


database.readConfigFromDB = function(onResults){
    var queryText = "SELECT * FROM Config";
    connection.query(queryText, function (error, results) {
        if (error) throw error;
        if (typeof onResults === "function") {
            onResults(results);
        }
    });
}

database.query = function(queryText, onResults){
    connection.query(queryText, function (error, results, fields) {
        if (error) throw error;
        if (typeof onResults === "function") {
            onResults(results, fields);
        }
    });
}

database.execute = function(queryText, onResults){
    connection.execute(queryText, function (error, results) {
        if (error) throw error;
        if (typeof onResults === "function") {
            onResults(results);
        }
    });
}

database.end = function(callback){
    connection.end();
}

module.exports = database;