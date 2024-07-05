

if (process.env.NODE_ENV !== 'production') {
    require('dotenv').load();
}

var dbConfig = {
    user: "microfeeder",
    password: "SHZ9X9$P",
    server: "mssql8.wannafind.dk",
    database: "microfeeder_com"
};

var timeStamp = new Date().toISOString();
const fieldsConfig = ['name1', 'name2', 'supplement1perdose', 'supplement2perdose', 'dosespervisit1', 'dosespervisit2', 'default1perday', 'default2perday', '24hoursvisitsmax', 'rollingperiod', 'startday1', 'endday1', 'startday2', 'endday2']
const fieldsCows = ['tagID', 'animalID', 'calvingdate', '24hoursallowancesup1', '24hoursallowancesup2', 'accumulatedallowancesup1', 'accumulatedallowancesup2', 'visitswithin24hours1', 'visitswithin24hours2', 'totalvisits1', 'totalvisits2']
const fieldsFeeders = ['SlaveID', 'Grp']


var sql = require("mssql");
var sql1 = require("mssql");

const Json2csvParser = require('json2csv').Parser;


const container = "config";
const storage = require('azure-storage');
const blobService = storage.createBlobService();


const uploadString = async (containerName, blobName, text) => {
    return new Promise((resolve, reject) => {
        blobService.createBlockBlobFromText(containerName, blobName, text, err => {
            if (err) {
                reject(err);
            } else {
                resolve({ message: `Text "${text}" is written to blob storage` });
            }
        });
    });
};

const getConfigs = async function () {
    //query = "SELECT a.*, b.SN_Members FROM PitstopPLUS_Herds a inner join PitstopPLUS_Members b on a.MemberIndex_herds = b.MemberIndex";
    // supplementperdose set to 9999 instead of 0 to avoid div by zero error if not set
    query = `SELECT [MemberIndex_Herds]
      ,[name1]
      ,[name1a]
      ,[name2]
      ,[name2a]
      ,[group1name]
      ,[group2name]
      ,IIF(supplement1perdose > 0, supplement1perdose, 9999) as supplement1perdose              
      ,IIF(supplement1aperdose > 0, supplement1aperdose, 9999) as supplement1aperdose
      ,IIF(supplement2perdose > 0, supplement2perdose, 9999) as supplement2perdose
      ,IIF(supplement2aperdose > 0, supplement2aperdose, 9999) as supplement2aperdose
      ,[dosespervisit1]
      ,[dosespervisit1a]
      ,[dosespervisit2]
      ,[dosespervisit2a]
      ,[default1perday]
      ,[default1aperday]
      ,[default2perday]
      ,[default2aperday]
      ,[24hoursvisitsmax]
      ,[rollingperiod]
      ,[rollingperiod2]
      ,[24hoursvisitsmax2]
      ,[startday1]
      ,[startday1a]
      ,[endday1]
      ,[endday1a]
      ,[startday2]
      ,[startday2a]
      ,[endday2]
      ,[endday2a]
      ,'deprecated' AS kalibmode
      ,[systemmode_Lactating]
      ,[systemmode_Dry]
      ,b.SN_Members 
      FROM PitstopPLUS_Herds a inner join PitstopPLUS_Members b on a.MemberIndex_herds = b.MemberIndex`;
    /* sql.connect(dbConfig, function (err) {
        if (err) {
            console.log("Error while connecting database :- " + err);
            context.log(err);
        }
        else { */
            // create Request object
            var request = new sql.Request();
            // query to the database
            request.query(query, function (err, res) {
                //console.log(query);
                if (err) {
                    console.log("Error while querying database :- " + err);
                    context.log(err)
                    //sql.close();
                }
                else {
                    //context.log(res);
                    res.recordset.forEach(function (row) {
                        const json2csvParser = new Json2csvParser({ fieldsConfig, quote: '', delimiter: ';' });
                        const csv = json2csvParser.parse(row);
                        uploadString(container, row['SN_Members'] + 'A.csv', json2csvParser.parse(row));
                        console.log('Uploaded: ' + row['SN_Members'] + 'A.csv');
                        getCowlist(row['MemberIndex_Herds'], row['SN_Members']);
                        getFeederlist(row['MemberIndex_Herds'], row['SN_Members']);
                        //console.log(require('crypto').createHash('md5').update(csv).digest("hex"));
                    });

                    //sql.close();
                }
            });
       /* }
    });*/
}

const getFeederlist = async function (memberIndex, serialNumber) {
    query = "SELECT SlaveID, [Group] as Grp FROM PitstopPLUS_Feeders  WHERE MemberIndex_Feeders = " + memberIndex;
            // create Request object
            var request = new sql1.Request();
            // query to the database
            request.query(query, function (err, res) {
                //console.log(query);
                if (err) {
                    console.log("Error while querying database :- " + err);
                    context.log(err)
                    //sql1.close();
                }
                else {
                    //console.log(res);
                    try {
                        const json2csvParser = new Json2csvParser({ fieldsFeeders, quote: '', delimiter: ';' });
                        const csv = json2csvParser.parse(res.recordset);
                        uploadString(container, serialNumber + 'C.csv', csv);
                        console.log('Uploaded: ' + serialNumber + 'C.csv');
                        //console.log(require('crypto').createHash('md5').update(csv).digest("hex"));
                        //sql1.close();
                    } catch (err) {
                        console.log ("Parser error (feeders): " + err);
                    }
                }
            });
}


const getCowlist = async function (memberIndex, serialNumber) {
    query = "SELECT *, visitswithin24hours1 = 0, visitswithin24hours2 = 0, totalvisits1 = 0, totalvisits2 = 0 FROM PitstopPLUS_Cows WHERE MemberIndex_Cows = " + memberIndex;
            // create Request object
            var request = new sql1.Request();
            // query to the database
            request.query(query, function (err, res) {
                //console.log(query);
                if (err) {
                    console.log("Error while querying database :- " + err);
                    context.log(err)
                    //sql1.close();
                }
                else {
                    //context.log(res);
                    try {
                        const json2csvParser = new Json2csvParser({ fieldsCows, quote: '', delimiter: ';' });
                        const csv = json2csvParser.parse(res.recordset);
                        uploadString(container, serialNumber + 'B.csv', csv);
                        console.log('Uploaded: ' + serialNumber + 'B.csv');
                        //console.log(require('crypto').createHash('md5').update(csv).digest("hex"));
                        //sql1.close();
                    } catch (err) {
                        console.log ("Parser error (cowlist): " + err);
                    }
                }
            });
}


module.exports = async function (context, myTimer) {

    if (myTimer.isPastDue) {
        context.log('JavaScript is running late!');
    }
    context.log('JavaScript timer trigger function ran!', timeStamp);

    try {
        sql.close();
    }
    catch (err) {
        console.log("Bugger");
    }

    sql.connect(dbConfig, function (err) {
        if (err) {
            console.log("Error while connecting database :- " + err);
            sql.close();
            context.log(err);
        }
        else {
            getConfigs();
        }
    });

};