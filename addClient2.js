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

const client = new AWS.ApiGatewayManagementApi({endpoint:url})



//query ddb
//@params - Key expression(string)
//          - Expression Values(object)
async function queryDb(keyExp,expValues,filterExp){ 
    filterExp = typeof filterExp=='undefined'?null:filterExp;
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


//@desc - get name for a userId
//params - userId(string)
async function getUserName(userId){
    return new Promise((resolve,reject)=>{
        const query= `SELECT firstName,lastName FROM User WHERE BIN_TO_UUID(id)="${userId}"`
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
    console.log(uidList)
    uidList = uidList.filter((uid)=>{   //for all non-null uids
        return uid!=null
    })
    let allProm = uidList.map(uid=>getUserName(uid))
    return Promise.all(allProm)
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

//add clientID to ddb
// @params  - orderId    (Partition Key)
//          - connectionId  (Sort Key)
//          - userId (string)
//          - username(string)
//          - guestUser(bool)
async function addClient(orderId,connectionId,userId,username=null,guestUser=false){
    const params ={
        TableName:process.env.TABLE_NAME,
        Item:{
            'orderId':orderId,
            'connectionId':connectionId,
            'userId':userId,
            'username':username,
            'guestUser':guestUser
        }
    };
    try{
        await db.put(params).promise();
    }
    catch(err){
        console.log(err,"Failed to Put to DB");
    }
}

async function getGuestName(userIDs){
    let numOfGuests = await userIDs.filter((id)=>{
        return id==null;
    })
    return `Guest${numOfGuests.length+1}`
}

exports.handler = async (event) => {
    
    const connectionId = event.requestContext.connectionId
    let orderId = JSON.parse(event.body).orderId
    let uid = event.requestContext.authorizer.principalId//JSON.parse(event.body).userId
    let usersByOrder={};
    
    if(uid) {  //for loggedin user
    
        //get clients in the orderId
         await queryDb("orderId =:orderId",{         
                    ":orderId":orderId
                 })
                 //map connectionId to userId for clients
                 .then(async(data)=>await mapper(data))
                 .then((mappedUsers)=>usersByOrder=mappedUsers)
                 
                 await getUserName(uid) //get name of newuser
                 .then(async(res)=>{
                      if(Object.keys(usersByOrder).length!=0){ // onlyif some client is connected
                      await sendToAll(Object.keys(usersByOrder),{msg:`${res[0].firstName} ${res[0].lastName} has joined`})
                      }
                  else return;
                 })
                 
                 await addClient(orderId,connectionId,uid)                                      // add connectionId to ddb
                  .then(async ()=> await queryDb("orderId =:orderId",{":orderId":orderId}))
                  .then(async(data)=>await mapper(data))                                        //maps connectionId to userId 
                  .then((mappedUsers)=>usersByOrder=mappedUsers)
                  
                  await getBatchUserNames(Object.values(usersByOrder))
                  .then(async(res)=>await namesMapper(res,usersByOrder,orderId,connectionId)) //returns a array of obj{userName:"",userId:""}
                  .then(async(users)=>sendToAll(Object.keys(usersByOrder),{activeUsers:users}))
    
    
    }else{ // if not uid
    
        let guestUsername ="";
        
        await queryDb("orderId =:orderId",{         
                    ":orderId":orderId
        })
        .then(async(data)=>await mapper(data))
        .then((mappedUsers)=>usersByOrder=mappedUsers)
        
        guestUsername = await getGuestName(Object.values(usersByOrder))

         if(Object.keys(usersByOrder).length!=0){ // onlyif some client is connected
            await sendToAll(Object.keys(usersByOrder),{msg:`${guestUsername} has joined`})
         };
         
         await addClient(orderId,connectionId,null,guestUsername,true)
        .then(async ()=> await queryDb("orderId =:orderId",{":orderId":orderId}))
        .then(async(data)=>await mapper(data))                                        //maps connectionId to userId 
        .then((mappedUsers)=>usersByOrder=mappedUsers)
                  
        .then(async()=>await getBatchUserNames(Object.values(usersByOrder)))
        .then(async(usernames)=>await namesMapper(usernames,usersByOrder,orderId,connectionId)) //returns a array of obj{userName:"",userId:""}
        .then(async(users)=>sendToAll(Object.keys(usersByOrder),{activeUsers:users}))
        
    }
    
    
                 
    // TODO implement  
    const response = {
        statusCode: 200,
        body: JSON.stringify('Connected'),
    };
    return response;
};
