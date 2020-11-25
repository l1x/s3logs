namespace S3logs

// internal
open Cli
open Logging

//external
open System
open Amazon
open Amazon.S3
open S3v2

module Main =

  let loggerMain =
    Logger.CreateLogger "Main" "info" (fun _ -> DateTime.Now)


  [<EntryPoint>]
  let main argv =
    let commandLineArgumentsParsed = parseCommandLine (Array.toList argv)

    loggerMain.LogInfo
    <| sprintf "%A" commandLineArgumentsParsed

    let awsProfile = commandLineArgumentsParsed.AwsProfile
    let awsRegion = commandLineArgumentsParsed.AwsRegion

    let credentials =
      AwsUtils.getAwsProfileCredentials awsProfile

    let region =
      RegionEndpoint.GetBySystemName(awsRegion)

    let config =
      AmazonS3Config(RegionEndpoint = region, Timeout = Nullable(TimeSpan.FromMilliseconds(500.0)))


    match credentials with
    | Some creds ->
        let client = new AmazonS3Client(creds, config)
        let s3v2 = S3v2.S3v2(client, loggerMain.LogInfo)
        // list files
        // download files
        // unzip files
        // merge files
        // convert files to parquet
        // upload to partition
        ()
    | None -> Environment.Exit 1

    0
