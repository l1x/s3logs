namespace S3logs

open System
open Amazon.Runtime.CredentialManagement
open Amazon.S3

module AwsUtils =

  let getAwsProfileCredentials profileName =
    try
      let sharedFile = SharedCredentialsFile()
      let success1, basicProfile = sharedFile.TryGetProfile(profileName)

      let success2, awsCredentials =
        AWSCredentialsFactory.TryGetAWSCredentials(basicProfile, sharedFile)

      match success1, success2 with
      | true, true -> Some(awsCredentials)
      | _, _ -> None
    with ex -> None
