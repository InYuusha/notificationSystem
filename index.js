
const AWS = require('aws-sdk') 
const mysql = require('mysql')
const connection = mysql.createConnection({
    host: process.env.RDS_HOSTNAME,
    user: process.env.RDS_USERNAME,  
    password: process.env.RDS_PASSWORD,
    port: process.env.RDS_PORT,
    database: process.env.RDS_DATABASE
  });

const db = new AWS.DynamoDB.DocumentClient(); 

const url = process.env.CONNECTION_URL;

const client = new AWS.ApiGatewayManagementApi({endpoint:url});

//@desc - get name for a userId
//params - userId(string)
async function getUserName(userId){
    return new Promise((resolve,reject)=>{
        const query= `SELECT firstName,lastName FROM User WHERE BIN_TO_UUID(id)="${userId}"`;
        connection.query(query,(err,res)=>{
            if(err){
                console.log(err)
            }
            resolve(res)
        })
    })
}
//@desc - get list of name for list of userId
//@parmas - uidList(array)
async function getBatchUserNames(uidList){
 
    let allProm = uidList.map(uid=>getUserName(uid))
    return Promise.all(allProm)
}


//get Items using scan(slower) 
//@params filterExp: filter Expression
//        expValues: Expression values

function getItems(filterExp,expValues){
    return new Promise(async(resolve,reject)=>{
        let params ={
            TableName:process.env.TABLE_NAME,
            FilterExpression:filterExp,
            ExpressionAttributeValues:expValues 
        };
        try{
            let data =await db.scan(params).promise()
            resolve(data)
        }
        catch(err){console.log(err)}
    })
}

//query ddb
//@params - Key expression(string)
//          - Expression Values(object)
//          - filterExpression(string)
async function queryDb(keyExp,expValues,filterExp){ 
    filterExp = typeof filterExp=='undefined'?null:filterExp; //null by default
    return new Promise(async (resolve,reject)=>{
          var params = {
          TableName: process.env.TABLE_NAME, 
          KeyConditionExpression: keyExp,
          FilterExpression:filterExp,
          ExpressionAttributeValues: expValues  //partition key
        };                                       // is must for query
        try{
            let data = await  db.query(params).promise();
            resolve(data)
        }
        catch(err){
            console.log(err,"cannot query")
        }
    })
}
//users mapper
//@desc - maps connectionId and userId 
//        for list of user objects
//@params - data(Object)
async function mapper(data){ 
    let users={} //empty it
    await data.Items.map(({connectionId,userId})=>{
           users[connectionId]=userId;
    });
    return users;
}

//@desc - returns a array of
//        obj containing
//        username , usersId and isCurrent(bool)
//@params - names(Object)
//        - usersByOrder(object)
//        - orderId(string)
//        - connId(string)    
async function namesMapper(names,userByOrder,orderId,connId){
    
    let activeUsers=[]
    let loggedInUserMp={} //maps connId to userId for loggedin users
    
    for(let key in userByOrder){       //filter userByOrder map for loggedin users only
        if(userByOrder[key]!==null){
            loggedInUserMp[key]=userByOrder[key]
        }
    }
    let connectionIDs = Object.keys(loggedInUserMp)
    let userIDs=Object.values(loggedInUserMp)
    
    let i=0;
    
    // add loggedin users to userNames
    await names.forEach(async(row)=>{     
        if(row.length!=0){  //if user is logged in users
            
            let obj={
                userId:userIDs[i],
                userName:`${row[0].firstName} ${row[0].lastName}`,
                isCurrent:connId===connectionIDs[i]
            };
            i++;
            activeUsers.push(obj) //populate usernames arrays
        }
    })
    
    //add guest users to userNames
    await queryDb('orderId=:orderId',{':orderId':orderId,':guestUser':true},'guestUser=:guestUser') 
    .then(async(res)=>{
        await res.Items.forEach(({username,connectionId})=>{
            let obj={
                userName:username,
                isCurrent:connId===connectionId
                
            }
            activeUsers.push(obj)
        })
    })
   
    return activeUsers
}
//@desc - remove client from ddb
//@params - orderId(string)
//        - connectionId(string)
async function remove(orderId,connectionId){
    let params={
        TableName:process.env.TABLE_NAME,
        Key:{
            'orderId':orderId,
            'connectionId':connectionId,
            
        }
    }
    try{
        await db.delete(params).promise()
    }
    catch(err){
        console.log(err,"cannot delete")
    }
}

//@desc - send to one client(used for send to all)
//@params - clientID(string)
//             - @desc - id of client to send data to
//        - data(Object)
//             - @desc - msg to send
const sendToOne = async(clientID,data)=>{
    try{
        await client.postToConnection({
            'ConnectionId': clientID,
            'Data': Buffer.from(JSON.stringify(data))
        }).promise()
    }
    catch(err){
        console.log(err)
    };
    
}
//@desc - send to list of clients
//params - clients(Array)
//       - data(Object)
const sendToAll = async (clients,data) => {
    const allProm = clients.map(clientID=>sendToOne(clientID,data))
    return Promise.all(allProm)
};


exports.handler = async (event) => {
    console.log(event);
    if(event.requestContext.connectionId){
        
        let orderId;
        let uid;
        let user={}
        
        const connectionId = event.requestContext.connectionId
        const routeKey = event.requestContext.routeKey
        
        switch(routeKey){
             
            case '$connect':  //on connect--->
                
                break;
                
            case '$disconnect':  //on disconnect---> 
                
                await getItems('connectionId=:id',{':id':connectionId}) //get user
                .then(data=>user=data.Items[0])
                
                if(typeof user!='undefined'){  //only if user exists
                
                    //get Items where connectionId is of disconnected user
                    await getItems('connectionId=:connectionId',{':connectionId':connectionId}) 
                    .then(async(res)=>{
                   
                    let allProm = res.Items.map(async({orderId,connectionId})=>{ //remove user(if exists) from all orderIDs
                        let usersByOrder={};                                    //notify the remaining clients
                        
                        await remove(orderId,connectionId)                      
                        .then(async()=>await queryDb("orderId =:orderId",{":orderId":orderId})) //get users with orderId
                        .then(async(data)=>await mapper(data))             
                        .then((mappedUsers)=>usersByOrder=mappedUsers)
                        
                       if(user.guestUser==true){
                           await sendToAll(Object.keys(usersByOrder),{msg:`${user.username} has left`})
                       }else{
                            await getUserName(user.userId)              //get name of disconnected user
                            .then(async(res)=>await sendToAll(Object.keys(usersByOrder),{msg:`${res[0].firstName} ${res[0].lastName} has left`}))
                       }
                        
                        await getBatchUserNames(Object.values(usersByOrder)) //get names of remaining users
                        .then(async (names)=>await namesMapper(names,usersByOrder,orderId,connectionId)) 
                        .then(async(users)=>await sendToAll(Object.keys(usersByOrder),{activeUsers:users})) 
                    })
                     await Promise.all(allProm) 
                  })
                }
             
                break;
                
           
            case 'deleteClient': // delete user from orderId
    
                orderId =JSON.parse(event.body).orderId;
                
                
                await queryDb("orderId=:orderId and connectionId=:id",{":orderId":orderId,":id":connectionId}) //get
                .then(data=>{user=data.Items[0];return})                                                      //user
               
                if(typeof user!='undefined'){   // if user exists
                    let usersByOrder=[];
                    
                     await remove(orderId,connectionId)       //remove that user
                    .then(async()=>await queryDb("orderId =:orderId",{":orderId":orderId}))  //get all remaining users
                    .then(async(data)=> await mapper(data))                                  // in that orderId
                    .then((mappedUsers)=>usersByOrder=mappedUsers)
                    
                    if(user.guestUser==true){
                        await sendToAll(Object.keys(usersByOrder),{msg:`${user.username} has left`})
                    }else{
                         await getUserName(user.userId)                                         //get names of users left
                        .then(async(name)=>await sendToAll(Object.keys(usersByOrder),{msg:`${name[0].firstName} ${name[0].lastName} has left`}))
                    }
                    
                    await getBatchUserNames(Object.values(usersByOrder))
                    .then(async(usernames)=>await namesMapper(usernames,usersByOrder,orderId,connectionId))
                    .then(async(users)=>await sendToAll(Object.keys(usersByOrder),{activeUsers:users}))
                }
                break;
                
            case '$default': 
                //default route
                console.log("Unknown Route hit")
                return{
                    statusCode:404,
                    body:"Not Found"
                }
        }
    }
    
    // TODO implement
    const response = {
        statusCode: 200,
        body: "Connected",
    };
    return response;
};
