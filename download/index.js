/*
  Main application
*/

const config = require('./config');
const cloud = require('./lib/cloud');
const lora = require('./lib/lora');
const database = require('./lib/database');
const exec = require('child_process').exec;

const FEED_DATA_SENT = 0
const FEED_ACKNOWLEDGED = 1
const COW_NOT_FOUND = 2
const SAME_COW_TOO_SOON = 3
const COW_24H_LIMIT_EXCEEDED = 4
const COW_ACC_IS_ZERO = 5
const TIME_BETWEEN_COWS = 6
const SLAVE_HEARTBEAT = 7
const LID_OPENED = 8
const LID_CLOSED = 9
const CALIBRATION_MODE = 10

const LED_OFF = 0
const LED_RED = 1
const LED_GREEN = 2
var app = {};

// Expected data is MASTERID:SLAVEID:COMMAND:PAYLOAD:IGNORED
app.processMsg = function(message) {
  app.setLED(LED_RED);
  setTimeout(function(){ app.setLED(LED_GREEN); }, 1000);
  
  if (config.deviceAddress !== message.substring(0,config.deviceAddress.length)) {
    console.log("Address mismatch");
    return;
  }
  var rxInfo = message.split(":");
  if (rxInfo.length<4) {
    console.log("Too few arguments");
    return;
  }
  var slave = rxInfo[1];
  var cmd = rxInfo[2];
  var payload = rxInfo[3];

  switch(cmd) {
    case 'A':
      app.startDispenseProcedure(payload, slave);
      break;
    case 'B':
      app.testRadioConnection(slave, payload)      
      break;
    case 'C':
      app.ackMineralsDispensed(slave, payload)
      break;
    default:
      console.log ("No valid command received")
      console.log(message);
    }

};
app.setLED = function (ledColor) {
  if (ledColor == LED_GREEN) {
    exec('fast-gpio set 2 0');
    exec('fast-gpio set 3 1');
  } else if (ledColor == LED_RED) {
    exec('fast-gpio set 2 1');
    exec('fast-gpio set 3 0');
  } else {
    exec('fast-gpio set 2 0');
    exec('fast-gpio set 3 0');
  }
}


app.saveActivity = function (eartag, slaveID, gramSup1, gramSup2, comment, activityCode, syncronized) {
  var queryText = "INSERT INTO CowActivity (Eartag, VisitTime, GramSup1, GramSup2, Comment, Syncronized, SlaveID, Herd, Country, ActivityCode) ";
  queryText += "VALUES ('" + eartag + "', NOW(), " + gramSup1 + ", " + gramSup2 + ", '" + comment + "', " + syncronized + ", '" + slaveID + "', '" + config.herd + "', '" + config.country + "', " + activityCode + ")";
  //console.log("Executing: " + queryText);
  database.execute(queryText);
  //app.sendActivity();
}

app.dispenseError = function (eartag, slaveID, activityCode) {
  app.saveActivity(eartag, slaveID, 0, 0, '', activityCode, 0);
}


app.dispenseMinerals = function(eartag, slaveID, doseSup1, doseSup2) {

  //# Payload: Motor1_reverse_time#Motor1_forward_time#Motor2_reverse_time#Motor2_forward_time
  //# All times are x100 milliseconds. Numbers are 10-based
  
  var reverseMotor1 = 0;
  var reverseMotor2 = 0;
  var grams1 = Math.round(doseSup1 * app.parameters['GramsPerDoseSup1']);
  var grams2 = Math.round(doseSup2 * app.parameters['GramsPerDoseSup2']);

  console.log("To be fed supplement 1: " + grams1);
  console.log("To be fed supplement 2: " + grams2);
  reverseMotor1 = (doseSup1 > 0) ? config.defaultReverseValue : 0;
  reverseMotor2 = (doseSup2 > 0) ? config.defaultReverseValue : 0;
  dispenseString = reverseMotor1 + "#" + Math.round(doseSup1 * config.doseTime * 10) + "#" + reverseMotor2 + "#" + Math.round(doseSup2 * config.doseTime * 10);
  app.sendRadioMessage(slaveID, "A", dispenseString);
  console.log("Sent feed info");
  console.log(dispenseString);
  app.saveActivity(eartag, slaveID, doseSup1 * app.parameters['GramsPerDoseSup1'], doseSup2 * app.parameters['GramsPerDoseSup2'], '', FEED_DATA_SENT, 1);
};

app.getSupplement2Amount = function (eartag, slaveID, accDays1, accDays2, sup1DailyGrams, sup2DailyGrams, doseSup1) {
  queryText = "SELECT SUM(GramSup2) FedInAccPeriod FROM CowActivity WHERE Eartag = '" + eartag + "' AND VisitTime >= ADDDATE(NOW(), " + accDays2 * -1 + ") AND ActivityCode = " + FEED_ACKNOWLEDGED;
  database.query(queryText, function(rows, fields){
    if (rows.length > 0) {
      rows.forEach(function(row) {
        fedInAccPeriod = row['FedInAccPeriod'];
        if (fedInAccPeriod == null) fedInAccPeriod = 0;
        console.log("Grams Sup 2 fed in acc period: " + fedInAccPeriod);
        doseSup2 = ((sup2DailyGrams * (accDays2)) - fedInAccPeriod) / app.parameters['GramsPerDoseSup2'];
        if (doseSup2 > app.parameters['DosesPerVisitSup2']) doseSup2 = app.parameters['DosesPerVisitSup2'];
        if (doseSup2 < 0) doseSup2 = 0;
        app.dispenseMinerals(eartag, slaveID, doseSup1, doseSup2);
      });
    }
    else
    {
      console.log("Unable to calculate number of doses for sup 2");
    }
  });
}

app.getSupplement1Amount = function (eartag, slaveID, accDays1, accDays2, sup1DailyGrams, sup2DailyGrams) {
  queryText = "SELECT SUM(GramSup1) FedInAccPeriod FROM CowActivity WHERE Eartag = '" + eartag + "' AND VisitTime >= ADDDATE(NOW(), " + accDays1 * -1 + ") AND ActivityCode = " + FEED_ACKNOWLEDGED;
  database.query(queryText, function(rows, fields){
    if (rows.length > 0) {
      rows.forEach(function(row) {
        fedInAccPeriod = row['FedInAccPeriod'];
        if (fedInAccPeriod == null) fedInAccPeriod = 0;
        console.log("Grams Sup 1 fed in acc period: " + fedInAccPeriod);
        doseSup1 = ((sup1DailyGrams * (accDays1)) - fedInAccPeriod) / app.parameters['GramsPerDoseSup1'];
        if (doseSup1 > app.parameters['DosesPerVisitSup1']) doseSup1 = app.parameters['DosesPerVisitSup1'];
        if (doseSup1 < 0) doseSup1 = 0;
        app.getSupplement2Amount(eartag, slaveID, accDays1, accDays2, sup1DailyGrams, sup2DailyGrams, doseSup1);
      });
    }
    else
    {
      console.log("Unable to calculate number of doses for sup 1");
    }
  });
}

app.getDailyDoses = function (eartag, slaveID, accDays1, accDays2) {
  queryText = "SELECT Feed24hrSup1, Feed24hrSup2 from CowList WHERE Eartag = '" + eartag + "' LIMIT 1"
  database.query(queryText, function(rows, fields){
    if (rows.length > 0) {
      rows.forEach(function(row) {
        console.log("Daily Dose Sup 1 (gram): " + row['Feed24hrSup1']);
        console.log("Daily Dose Sup 2 (gram): " + row['Feed24hrSup2']);
        app.getSupplement1Amount(eartag, slaveID, accDays1, accDays2, row['Feed24hrSup1'], row['Feed24hrSup2']);
      });
    }
    else
    {
      console.log("Unable to calculate daily doses");
    }
  });
}

app.getAccumulationDays = function (eartag, slaveID) {
  accDays1 = 0;
  accDays2 = 0;
  queryText = "SELECT CalvingDate, DATEDIFF(NOW(), CalvingDate) ThisDaysNumber "
  queryText += " FROM CowList WHERE Eartag = '" + eartag + "' LIMIT 1";
  console.log(queryText);
  database.query(queryText, function(rows, fields){
    if (rows.length > 0) {
      rows.forEach(function(row) {
        console.log("Calving Date: " + row['CalvingDate']);
        console.log("This Days Number: " + row['ThisDaysNumber']);
        // Supplement 1
        accDays1 = Number(row['ThisDaysNumber']) - app.parameters['StartDaySup1'];
        if (accDays1 > app.parameters['AccumulationDays']) accDays1 = app.parameters['AccumulationDays'];
        if (row['ThisDaysNumber'] > app.parameters['EndDaySup1'] || row['ThisDaysNumber'] < app.parameters['StartDaySup1']) accDays1 = -1;
        if (accDays1 == NaN) accDays1 = -1;
        // Supplement 2
        accDays2 = Number(row['ThisDaysNumber']) - app.parameters['StartDaySup2'];
        if (accDays2 > app.parameters['AccumulationDays']) accDays2 = app.parameters['AccumulationDays'];
        if (row['ThisDaysNumber'] > app.parameters['EndDaySup2']  || row['ThisDaysNumber'] < app.parameters['StartDaySup2']) accDays2 = -1;
        if (accDays2 == NaN) accDays2 = -1;
        if (accDays1 <= 0 && accDays2 <= 0) {
          app.dispenseError(eartag, slaveID, COW_ACC_IS_ZERO);
        }
        else {
          console.log("Acc days 1: " + accDays1);
          console.log("Acc days 2: " + accDays2);
          app.getDailyDoses(eartag, slaveID, accDays1, accDays2);
        }
      });
    }
    else
    {
      console.log("Unable to calculate accumulation days");
    }
  });
}

app.check24HourLimitReached = function (eartag, slaveID) {
  queryText = "SELECT COUNT(*) Visits FROM CowActivity WHERE Eartag = '" + eartag + "' AND ActivityCode = " + FEED_ACKNOWLEDGED + " AND VisitTime >= ADDDATE(NOW(), -1) LIMIT 1";
  database.query(queryText, function(rows, fields){
    if (rows.length>0) {
      rows.forEach(function(row) {
        console.log("Visits within 24 hours is: " + row['Visits']);
        if (Number(row['Visits']) >= app.parameters['MaxDosesPer24hr']) {
          console.log("24 hour limit (" + app.parameters['MaxDosesPer24hr'] + ") reached");
          app.dispenseError(eartag, slaveID, COW_24H_LIMIT_EXCEEDED);
        }
        else
        {
          console.log("24 hour limit (" + app.parameters['MaxDosesPer24hr'] + ") not reached");
          app.getAccumulationDays(eartag, slaveID);
        }
      });
    }
    else
    {
      app.getAccumulationDays(eartag, slaveID);
    }
  });
}



app.checkSecondsSinceLastVisitThisCow = function (eartag, slaveID) {
  queryText = "SELECT TIME_TO_SEC(TIMEDIFF(NOW(), VisitTime)) Diff FROM CowActivity WHERE Eartag = '" + eartag + "' AND ActivityCode = " + FEED_ACKNOWLEDGED + " ORDER BY VisitTime DESC LIMIT 1"
  database.query(queryText, function(rows, fields){
    if (rows.length>0) {
      rows.forEach(function(row) {
        console.log("Time since this cow visited is " + row['Diff'] + " seconds.");
        if (row['Diff'] > config.delaySameCow) {
          app.check24HourLimitReached(eartag, slaveID);
        }
        else
        {
          console.log("Cow came back too soon");
          app.dispenseError(eartag, slaveID, SAME_COW_TOO_SOON);
        }
      });
    }
    else
    {
      app.check24HourLimitReached(eartag, slaveID);
    }
  });
}

app.checkSecondsSinceLastVisitAnyCow = function (eartag, slaveID) {
  queryText = "SELECT TIME_TO_SEC(TIMEDIFF(NOW(), VisitTime)) Diff FROM CowActivity WHERE ActivityCode = '" + FEED_ACKNOWLEDGED + "' ORDER BY VisitTime DESC LIMIT 1"
  database.query(queryText, function(rows, fields){
    if (rows.length>0) {
      rows.forEach(function(row) {
        console.log("Time since a cow visited is " + row['Diff'] + " seconds.");
        if (row['Diff'] < config.delayAfterCow) {
          console.log("Too short time since a cow was here");
          app.dispenseError(eartag, slaveID, TIME_BETWEEN_COWS);
        }
        else
        {
          app.checkSecondsSinceLastVisitThisCow(eartag, slaveID);
        }
      });
    }
    else
    {
      app.checkSecondsSinceLastVisitThisCow(eartag, slaveID);
    }
  });
}

app.checkCowExists = function (eartag, slaveID) {
  queryText = "SELECT * FROM CowList WHERE Eartag = '" + eartag + "'"
  database.query(queryText, function(rows, fields){
    if (rows.length>0) {
      console.log("Cow was found");
      app.checkSecondsSinceLastVisitAnyCow(eartag, slaveID);
    }
    else
    {
      console.log("Cow was not found");
      app.dispenseError(eartag, slaveID, COW_NOT_FOUND);
    }
  });
}



app.startDispenseProcedure = function (eartag, slaveID) {
  
  if (app.parameters['CalibrationMode']) {
    dispenseString = "0#" + (config.doseTime * 10) + "#0#" + (config.doseTime * 10)
    console.log("Calibration mode");
    app.saveActivity(eartag, slaveID, 0, 0, '', CALIBRATION_MODE, 0);
    app.sendRadioMessage(slaveID, "A", dispenseString);
  }
  else {
    app.checkCowExists(eartag, slaveID) 
  }

}



app.testRadioConnection = function(slaveID, payload) {
    app.saveActivity(payload, slaveID, 0, 0, 'Radio Check', SLAVE_HEARTBEAT, 0);
  //app.sendRadioMessage(slaveID, "B", "RADIO_OK")
};

app.ackMineralsDispensed = function(slaveID, payload) {
  //This message tells us that slave has dispensed according to instructions
  if (app.parameters['CalibrationMode']) return; // In case of calibration mode the ack is not logged
  var rxStr = payload.split("=");
  if (rxStr.length<2) {
    console.log("Wrong rxStr arguments");
    return;
  }
  var rxData = rxStr[1].split("#");
  if (rxData.length<4) {
    console.log("Wrong rxData arguments");
    return;
  }
  var eartag = rxStr[0];
  console.log("Dispense acknowledged for: " + rxStr[0])
  app.saveActivity(rxStr[0], slaveID, (app.parameters.GramsPerDoseSup1 * (parseInt(rxData[1],10)||0) / (config.doseTime * 10)), (app.parameters.GramsPerDoseSup2 * (parseInt(rxData[3],10)||0) / (config.doseTime * 10)), 'Confirmed feed', FEED_ACKNOWLEDGED, 0);

};

app.sendRadioMessage = function(slaveID, command, payload) {
  var message = slaveID + ":" + config.deviceAddress + ":" + command + ":" + payload + "\0";
  console.log("Sending: " + message);
  lora.send(message);
};



app.sendActivity = function() {

  sendObj = {};
  sendObj.Telemetry = [];
  sentIDs = [];
  database.query('SELECT * FROM CowActivity WHERE Syncronized=0 ORDER BY VisitTime LIMIT 1000', function(rows, fields){
    if (rows.length>0) {
      rows.forEach(function(row) {
        sendObj.Telemetry.push({
          earTag : row['Eartag'],
          visitTime : row['VisitTime'],
          gramSup1 : row['GramSup1'],
          gramSup2 : row['GramSup2'],
          comment : row['Comment'],
          slaveId : row['SlaveID'],
          herd : row['Herd'],
          country : row['Country'],
          activityCode : row['ActivityCode']
        });
        sentIDs.push(row['ID']);
		console.log('VISITTIME: ' + row['VisitTime']);
      });
  
      console.log("APP : Sending to Hub: ")
      cloud.send(sendObj, function() {
        console.log("APP : Sent to Hub");
        database.query('UPDATE CowActivity SET Syncronized = 1 WHERE ID IN ('+sentIDs.join()+')', function(result){
          console.log("APP : DB Updated ")
        });
      });
    }
  });
};

app.parameters = {};
app.comment = "";

app.init = function() {


  const onCloudConnected =  function() { 
    console.log("APP : Hub Connected");
  };

  const onCloudMessage =  function(message) { 
    console.log("APP : Message from the Hub", message);
  };

  const onLoraConnected =  function() { 
    cloud.init(onCloudConnected, onCloudMessage, onCloudTwinUpdate);
  };

  const onLoraMessage =  function(message) { 
    app.processMsg(message);
  };

  const onDatabaseInit =  function() {
    database.readConfigFile(onReadConfigFile, onDBerror);
    database.readCowListFile();
    setInterval(app.sendActivity, 10000);
  };

  const onReadConfigFile =  function() { 
    database.readConfigFromDB(onReadConfigFromDB);
  };

  const onReadConfigFromDB =  function(rows) { 
    app.parameters = rows[0];
  };

  const onDBerror = function(err) {
    console.log("Error from DB: " + err);
  }

  const onCloudTwinUpdate = function() {
    database.readConfigFile(onReadConfigFile, onDBerror);
    database.readCowListFile();
  }


  
  database.init(onDatabaseInit);

  lora.init(onLoraConnected, onLoraMessage);
  app.setLED(LED_GREEN);
};

app.init();
