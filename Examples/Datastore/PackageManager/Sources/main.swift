/*
 * Copyright 2017, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import Foundation
import gRPC
import OAuth2

let CREDENTIALS = "google.yaml" // in $HOME/.credentials
let TOKEN = "google.json" // local auth token storage

func main () throws {
  let arguments = CommandLine.arguments

  if arguments.count == 1 {
    print("Usage: \(arguments[0]) [options]")
    return
  }

  #if os(OSX)
    // On OS X, we use the local browser to help the user get a token.
    let tokenProvider = try BrowserTokenProvider(credentials:CREDENTIALS, token:TOKEN)
    if tokenProvider.token == nil {
      try tokenProvider.signIn(scopes:["profile",
                                       "https://www.googleapis.com/auth/contacts.readonly",
                                       "https://www.googleapis.com/auth/cloud-platform"])
      try tokenProvider.saveToken(TOKEN)
    }
  #else
    // On Linux, we can get a token if we are running in Google Cloud Shell
    // or in some other Google Cloud instance (GAE, GKE, GCE, etc).
    let tokenProvider = try GoogleTokenProvider()
  #endif

  gRPC.initialize()

  guard let authToken = tokenProvider.token?.accessToken else {
    print("ERROR: No OAuth token is avaiable.")
    exit(-1)
  }

  let projectID = "hello-86"
  let certificateURL = URL(fileURLWithPath:"roots.pem")
  let certificates = try! String(contentsOf: certificateURL, encoding: .utf8)
  let service = Google_Datastore_V1_DatastoreService(address:"datastore.googleapis.com",
                                                     certificates:certificates,
                                                     host:nil)

  service.metadata = Metadata(["authorization":"Bearer " + authToken])

  switch arguments[1] {

  case "query":
    var queryRequest = Google_Datastore_V1_RunQueryRequest()
    queryRequest.projectID = projectID
    var query = Google_Datastore_V1_GqlQuery()
    query.queryString = "select *"
    queryRequest.gqlQuery = query
    print("\(queryRequest)")
    let queryResult = try service.runquery(queryRequest)
    print("\(queryResult)")

  case "create":
    var beginTransactionRequest = Google_Datastore_V1_BeginTransactionRequest()
    beginTransactionRequest.projectID = projectID
    let result = try service.begintransaction(beginTransactionRequest)
    print("\(result)")

    var commitRequest = Google_Datastore_V1_CommitRequest()
    commitRequest.projectID = projectID
    commitRequest.transaction = result.transaction

    var nameValue = Google_Datastore_V1_Value()
    nameValue.stringValue = "Anonymous Person"
    var entity = Google_Datastore_V1_Entity()
    entity.properties["name"] = nameValue
    var key = Google_Datastore_V1_Key()
    var pathElement = Google_Datastore_V1_Key.PathElement()
    pathElement.kind = "Person"
    key.path.append(pathElement)
    entity.key = key

    var mutation = Google_Datastore_V1_Mutation()
    mutation.insert = entity

    commitRequest.mutations.append(mutation)
    let commitResult = try service.commit(commitRequest)
    print("\(commitResult)")

  case "delete":
    var beginTransactionRequest = Google_Datastore_V1_BeginTransactionRequest()
    beginTransactionRequest.projectID = projectID
    let result = try service.begintransaction(beginTransactionRequest)
    print("\(result)")

    var deleteRequest = Google_Datastore_V1_CommitRequest()
    deleteRequest.projectID = projectID
    deleteRequest.transaction = result.transaction

    var key = Google_Datastore_V1_Key()
    var pathElement = Google_Datastore_V1_Key.PathElement()
    pathElement.kind = "Person"
    pathElement.id = Int64(arguments[2])!
    key.path.append(pathElement)

    var mutation = Google_Datastore_V1_Mutation()
    mutation.delete = key

    deleteRequest.mutations.append(mutation)
    let deleteResult = try service.commit(deleteRequest)
    print("\(deleteResult)")

  default:
    print("unknown command: \(arguments)")
  }
}

do {
  try main()
} catch (let error) {
  print("ERROR: \(error)")
}
