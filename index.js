const AWS = require('aws-sdk');
const mysql = require('mysql');
const connection = mysql.createConnection({
    host: process.env.RDS_HOSTNAME,
    user: process.env.RDS_USERNAME,
    password: process.env.RDS_PASSWORD,
    port: process.env.RDS_PORT,
    database: process.env.RDS_DATABASE
});

const db = new AWS.DynamoDB.DocumentClient();

const url = process.env.CONNECTION_URL;

const client = new AWS.ApiGatewayManagementApi({ endpoint: url });

//query ddb
//@params - Key expression(string)
//          - Expression Values(object)
async function queryDb(keyExp, expValues, filterExp) {
    filterExp = typeof filterExp == 'undefined' ? null : filterExp;
    return new Promise(async(resolve, reject) => {
        var params = {
            TableName: process.env.TABLE_NAME,
            KeyConditionExpression: keyExp,
            FilterExpression: filterExp,
            ExpressionAttributeValues: expValues //partition key
        }; // is must for query
        try {
            let data = await db.query(params).promise();
            resolve(data);
        }
        catch (err) {
            console.log(err, "cannot query");
        }
    });
}


//@desc - get name for a userId
//params - userId(string)
async function getUserName(userID) {
    return new Promise((resolve, reject) => {
        const query = `SELECT CONCAT(firstName," ",lastName) as userName FROM User WHERE BIN_TO_UUID(id)=?`;
        connection.query(query, [userID], (err, res) => {
            if (err) {
                console.log(err);
            }
            resolve(res[0]);
        });
    });
}

async function getItemsByOrderId(orderId) {
    return await queryDb("orderId =:orderId", { ":orderId": orderId });
}

async function getItemsByConnectionId(connectionId) {
    return new Promise(async(resolve, reject) => {
        var params = {
            TableName: process.env.TABLE_NAME,
            IndexName: "connectionId-index",
            KeyConditionExpression: "connectionId = :connectionId",
            ExpressionAttributeValues: {
                ":connectionId": connectionId
            },
            ProjectionExpression: "orderId, connectionId",
            ScanIndexForward: false
        };
        try {
            let data = await db.query(params).promise();
            resolve(data);
        }
        catch (err) {
            console.log(err, "cannot query");
        }
    });
}

// async function getItemsByConnectionId(filterExp,expValues){
//     return new Promise(async(resolve,reject)=>{
//         let params ={
//             TableName:process.env.TABLE_NAME,
//             FilterExpression:filterExp,
//             ExpressionAttributeValues:expValues 
//         };
//         try{
//             let data =await db.scan(params).promise()
//             resolve(data)
//         }
//         catch(err){console.log(err)}
//     })
// }

//@params - clientID(string)
//             - @desc - id of client to send data to
//        - data(Object)
//             - @desc - msg to send
const sendToOne = async(clientID, data) => {
    try {
        await client.postToConnection({
            'ConnectionId': clientID,
            'Data': Buffer.from(JSON.stringify(data))
        }).promise();
    }
    catch (err) {
        console.log(err);
    }
};

//@desc - send to list of clients
//params - clients(Array)
//       - data(Object)
const sendToAll = async(clients, data) => {
    const allProm = clients.map(clientID => sendToOne(clientID, data));
    return Promise.all(allProm);
};

//add item to ddb
// @params  - item (Object)
async function addItem(item) {
    const params = {
        TableName: process.env.TABLE_NAME,
        Item: item
    };
    try {
        await db.put(params).promise();
    }
    catch (err) {
        console.log(err, "Failed to Put to DB");
    }
}

//@desc - remove client from ddb
//@params - orderId(string)
//        - connectionId(string)
async function remove(orderId, connectionId) {
    let params = {
        TableName: process.env.TABLE_NAME,
        Key: {
            'orderId': orderId,
            'connectionId': connectionId,
        }
    };
    try {
        await db.delete(params).promise();
    }
    catch (err) {
        console.log(err, "cannot delete");
    }
}

async function sendActiveUsers(orderId) {
    const data = await getItemsByOrderId(orderId);
    console.log(data);
    for (let currentItem of data.Items) {
        const activeUsers = data.Items.map(item => {
            let obj = {};
            if (item.connectionId === currentItem.connectionId)
                obj.isCurrent = true;
            obj.userName = item.name;
            return obj;
        });
        await sendToOne(currentItem.connectionId, {
            "event": "activeUser",
            "message": activeUsers});
    }
}

exports.handler = async(event) => {

    const connectionId = event.requestContext.connectionId;
    const uid = event.requestContext.authorizer.principalId;
    const routeKey = event.requestContext.routeKey;
    let orderId;
    console.log(routeKey);

    switch (routeKey) {
        case '$connect': //on connect--->

            break;

        case '$disconnect':
            const data = await getItemsByConnectionId(connectionId);
            //   const data = await getItemsByConnectionId("connectionId=:connectionId",{':connectionId':connectionId});
            console.log(data);
            await Promise.all(data.Items.map(async(item) => {
                await remove(item.orderId, item.connectionId);
                await sendActiveUsers(item.orderId);
            }));
            break;

        case 'addClient':
            orderId = JSON.parse(event.body).orderId;
            if (uid) { //for loggedin user
                const name = await getUserName(uid);
                await addItem({ "orderId": orderId, "connectionId": connectionId, "userId": uid, "name": name.userName }); // add connectionId to ddb
            }
            else { // if not uid
                let guestUsername = "Guest";
                await addItem({ "orderId": orderId, "connectionId": connectionId, "name": guestUsername });
            }
            await sendActiveUsers(orderId);
            break;

        case 'deleteClient':
            orderId = JSON.parse(event.body).orderId;
            await remove(orderId, connectionId); //remove that user
            await sendActiveUsers(orderId);
            break;

        case '$default':
            //default route
            console.log("Unknown Route hit");
            return {
                statusCode: 404,
                body: JSON.stringify({"event": "Not Found"})
            };

    }
    const response = {
        statusCode: 200,
    };
    return response;

};
