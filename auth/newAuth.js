const jwt = require("jsonwebtoken");
const jwksClient = require('jwks-rsa')

const client = jwksClient({
  jwksUri:process.env.JWKS_URI
})
function generateAuthResponse(principalId, effect, methodArn) {
  const policyDocument = generatePolicyDocument(effect, methodArn);

  return {
    principalId,
    policyDocument
  };
}

function generatePolicyDocument(effect, methodArn) {
  if (!effect || !methodArn) return null;

  const policyDocument = {
    Version: "2012-10-17",
    Statement: [
      {
        Action: "execute-api:Invoke",
        Effect: effect,
        Resource: methodArn
      }
    ]
  };

  return policyDocument;
}
async function getKey(header){
  let key= await client.getSigningKey(header.kid)
  let signingKey = key.publicKey ||key.rsaPublicKey;
  
  return signingKey
  
}

exports.handler =  async(event, context, callback) => {
  const token = event.queryStringParameters.token 
  const methodArn = event.methodArn;
  const jwt_header = JSON.parse(new Buffer(token.split('.')[0],'base64').toString())
  console.log(token) 
  if (!token || !methodArn) return callback(null, "Unauthorized");
  //get the  public 
  let signingKey = await getKey(jwt_header)
  
  // get us payload
  const decoded=await jwt.verify(token, signingKey); //get us the payload
  
  if (decoded && decoded.sub) {
    return callback(null, generateAuthResponse(decoded.sub, "Allow", methodArn));
  } else {
    return callback(null, generateAuthResponse(decoded.sub, "Deny", methodArn));
  }
 
};