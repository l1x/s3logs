namespace S3logs

// internal
open Cli
open Logging
open S3v2

//external
open System
open Amazon
open Amazon.S3
open System.Text.RegularExpressions
open System.IO
open System.IO.Compression
open System.Collections.Generic

module Main =


  type FileState =
    | NotStarted
    | Downloaded
    | Unzipped
    | Merged
    | Uploaded
    | Done
    | FileStateError of err: string


  let loggerMain =
    Logger.CreateLogger "Main" "info" (fun _ -> DateTime.Now)


  let listFiles (s3v2: S3v2) (bucket: string) (folder: string) =
    loggerMain.LogInfo
    <| sprintf "%s %s" bucket folder
    let fileListMaybe = s3v2.ListS3Objects bucket folder
    match fileListMaybe with
    | Ok (S3ListSuccess (p, l)) -> Some(List.map (fun (x: Model.S3Object) -> x.Key) l)
    | Error err ->
        loggerMain.LogInfo <| sprintf "%A" err
        None


  let doListFiles (fileStates: Dictionary<string, FileState>) pattern s3v2 bucket folder =
    listFiles s3v2 bucket folder
    |> Option.map (List.filter (fun x -> Regex(pattern).Match(x).Success))
    |> Option.map (List.iter (fun f -> fileStates.Add(f, NotStarted)))
    |> Option.defaultWith (fun _ ->
         loggerMain.LogError "Could not list files"
         Environment.Exit 1)


  let downloadFile (s3v2: S3v2) (localFolder: string) (bucket: string) (key: string) =
    try
      let fileBytesMaybe = s3v2.GetS3ObjectBytes bucket key
      match fileBytesMaybe with
      | Ok (S3GetSuccess (k, v)) ->
          let fileName =
            sprintf "%s/dl/%s" localFolder (k.Replace('/', '_'))

          (new BinaryWriter(File.Open(fileName, FileMode.Create))).Write(v)
          Ok key
      | Error err ->
          loggerMain.LogError <| sprintf "%A" err
          Error(sprintf "%A" err)
    with ex ->
      loggerMain.LogError <| sprintf "%A" ex.Message
      Error ex.Message


  let doDownloadFiles (fileStates: Dictionary<string, FileState>) (s3v2: S3v2) (localFolder: string) (bucket: string) =
    for fileEntry in fileStates do
      match (downloadFile s3v2 localFolder bucket fileEntry.Key) with
      | Ok _x -> fileStates.[fileEntry.Key] <- Downloaded
      | Error err -> fileStates.[fileEntry.Key] <- (FileStateError err)


  let doUnzipFiles (fileStates: Dictionary<string, FileState>) localFolder =
    for fileEntry in fileStates do
      match fileEntry.Key, fileEntry.Value with
      | k, Downloaded ->
          try
            let inputFileName =
              sprintf "%s/dl/%s" localFolder (k.Replace('/', '_'))

            let outputFileName =
              inputFileName.Replace(".gz", "").Replace("/dl/", "/uz/")

            use inputStream =
              new StreamReader(new GZipStream(File.OpenRead(inputFileName), CompressionMode.Decompress))

            use outputStream = new StreamWriter(outputFileName)
            outputStream.Write(inputStream.ReadToEnd())

            fileStates.[fileEntry.Key] <- Unzipped
          with ex -> fileStates.[fileEntry.Key] <- FileStateError ex.Message
      | _, _ -> ()


  [<EntryPoint>]
  let main argv =
    let commandLineArgumentsParsed = parseCommandLine (Array.toList argv)

    loggerMain.LogInfo
    <| sprintf "%A" commandLineArgumentsParsed

    let awsProfile = commandLineArgumentsParsed.AwsProfile
    let awsRegion = commandLineArgumentsParsed.AwsRegion
    let month = commandLineArgumentsParsed.Month

    let credentials =
      AwsUtils.getAwsProfileCredentials awsProfile

    let region =
      RegionEndpoint.GetBySystemName(awsRegion)

    let config =
      AmazonS3Config(RegionEndpoint = region, Timeout = Nullable(TimeSpan.FromMilliseconds(1500.0)))

    let bucket = "logs.l1x.be"
    let folder = "dev.l1x.be"
    let localFolder = "tmp"

    match credentials with
    | Some creds ->

        let client = new AmazonS3Client(creds, config)
        let s3v2 = S3v2(client, loggerMain.LogInfo)
        let pattern = sprintf "^.*%s.*$" month
        let fileStates = new Dictionary<string, FileState>()

        doListFiles fileStates pattern s3v2 bucket folder

        loggerMain.LogInfo <| sprintf "%A" fileStates

        doDownloadFiles fileStates s3v2 localFolder bucket

        loggerMain.LogInfo <| sprintf "%A" fileStates

        doUnzipFiles fileStates localFolder

        loggerMain.LogInfo <| sprintf "%A" fileStates


        // merge files
        // convert files to parquet
        // upload to partition
        do ()

    | None -> Environment.Exit 1

    0
