module.exports = async function (context, eventHubMessages) {
  var sql = require('mssql');
  var sqlConfig = {
    user: 'microfeeder',
    password: 'SHZ9X9$P',
    server: 'mssql8.wannafind.dk',
    database: 'microfeeder_com'
  };
  //context.log(`JavaScript eventhub trigger function called for message array ${eventHubMessages}`);

  const pool = new sql.ConnectionPool(sqlConfig);

  var conn = pool;

  conn.connect().then(function () {
    eventHubMessages.forEach((message, index) => {
      deviceID = message.deviceID
      console.log(JSON.stringify(message.deviceID));
      message.Telemetry.forEach(function (item, idx) {
        
        //context.log(idx);
        //context.log(item);
        (function () {


          // Insert the new record into the log
          var req = new sql.Request(conn);
          if (item.activityCode > 0) {
            var IotSyncronizedAt = item.IotSyncronizedAt ? item.IotSyncronizedAt : '2000-01-01';
            qry = `INSERT INTO PitstopPLUS_Log (EarTag, VisitTime, GramSup1, GramSup2, GramSup1a, GramSup2a, SlaveID, ActivityType, MemberIndex_Log, MasterSyncTime)              
            SELECT TOP 1 '${item.earTag}', '${item.visitTime}', ${item.gramSup1}, ${item.gramSup2}, ${item.gramSupA1}, ${item.gramSupA2}, '${item.slaveId}',${item.activityCode || null}, MemberIndex, '${IotSyncronizedAt}'   
            FROM PitstopPLUS_Members where SN_Members = '${deviceID}'`
            context.log(qry);
            req.query(qry).then(function (result) {
              console.log(result);
              //conn.close();
            })
              .catch(function (err) {
                console.log(err);
                //conn.close();
                
            });
          }

          // Check if the cow is new and should be inserted
          if (item.possiblyNewCow) {
            console.log("Inserting new cow");
            var req1 = new sql.Request(conn);
            qry = `INSERT INTO PitstopPLUS_Cows (MemberIndex_Cows, tagID, [24hoursallowancesup1], [24hoursallowancesup1a], [24hoursallowancesup2], [24hoursallowancesup2a]) 
            SELECT TOP 1 b.MemberIndex_Log, b.EarTag, c.default1perday, c.default1aperday, c.default2perday, c.default2aperday 
              FROM PitstopPLUS_Log b INNER JOIN PitstopPLUS_Herds c on b.MemberIndex_Log = c.MemberIndex_Herds
              WHERE b.EarTag = '${item.earTag}' AND b.EarTag NOT IN (SELECT tagID from PitstopPLUS_Cows WHERE MemberIndex_Cows = b.MemberIndex_Log)`
            console.log(qry);

            req1.query(qry).then(function (result) {
              console.log(result);
              //conn.close();
            })
              .catch(function (err) {
                console.log(err);
                //conn.close();
              });
          }
        })()
      })

    });  // foreach end

  })
    .catch(function (err) {
      console.log(err);
    });


};

