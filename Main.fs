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


  let doListFiles (fileStates: Dictionary<string, FileState>) pattern s3v2 bucket folder startingState =
    listFiles s3v2 bucket folder
    |> Option.map (List.filter (fun x -> Regex(pattern).Match(x).Success))
    |> Option.map (List.iter (fun f -> fileStates.Add(f, startingState)))
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


  let doMergeFiles (fileStates: Dictionary<string, FileState>) month (localFolder) =
    for fileEntry in fileStates do
      match fileEntry.Key, fileEntry.Value with
      | k, Unzipped ->
          try
            let inputFileName =
              sprintf "%s/uz/%s" localFolder (k.Replace('/', '_').Replace(".gz", ""))

            let outputFileName = sprintf "%s/merged/%s" localFolder month

            use outputStream = File.AppendText(outputFileName)
            outputStream.Write(File.ReadAllText(inputFileName))

            fileStates.[fileEntry.Key] <- Merged
          with ex -> fileStates.[fileEntry.Key] <- FileStateError ex.Message
      | _, _ -> ()


  let splitLine (withChar: char) (s: string) = s.Split(withChar)

  let splitLineWithTab (s: string) = splitLine '\t' s

  let startsWith (withChar: char) (x: string) = x.StartsWith(withChar)

  let startsWithHash (s: string) = startsWith '#' s

  let processLine (line: string array) =
    loggerMain.LogInfo
    <| sprintf "Number of lines %A" line
    Array.concat [| [| (sprintf "%sT%s" line.[0] line.[1]) |]
                    line.[2..] |]

  let doConvertFileToParquet localFolder month =
    let mergedFile = sprintf "%s/merged/%s" localFolder month

    let lines =
      File.ReadLines(mergedFile)
      |> Seq.filter (startsWithHash >> not)
      |> Seq.map (splitLineWithTab >> processLine)



    loggerMain.LogInfo <| sprintf "%A" lines
    ()

  let whatToExecute state fileStates pattern s3v2 bucket s3folder localFolder month =
    match state with
    | NotStarted ->
        doListFiles fileStates pattern s3v2 bucket s3folder state
        doDownloadFiles fileStates s3v2 localFolder bucket
        doUnzipFiles fileStates localFolder
        doMergeFiles fileStates month localFolder
    | Downloaded ->
        doListFiles fileStates pattern s3v2 bucket s3folder state
        doUnzipFiles fileStates localFolder
        doMergeFiles fileStates month localFolder
    | Unzipped ->
        doListFiles fileStates pattern s3v2 bucket s3folder state
        doMergeFiles fileStates month localFolder
    | Merged ->
        doListFiles fileStates pattern s3v2 bucket s3folder state
        doConvertFileToParquet localFolder month
    | _ -> Environment.Exit 1


  [<EntryPoint>]
  let main argv =
    let commandLineArgumentsParsed = parseCommandLine (Array.toList argv)

    loggerMain.LogInfo
    <| sprintf "%A" commandLineArgumentsParsed

    let awsProfile = commandLineArgumentsParsed.AwsProfile
    let awsRegion = commandLineArgumentsParsed.AwsRegion
    let month = commandLineArgumentsParsed.Month
    let startingState = commandLineArgumentsParsed.State

    let credentials =
      AwsUtils.getAwsProfileCredentials awsProfile

    let region =
      RegionEndpoint.GetBySystemName(awsRegion)

    let config =
      AmazonS3Config(RegionEndpoint = region, Timeout = Nullable(TimeSpan.FromMilliseconds(1500.0)))

    let bucket = "logs.l1x.be"
    let s3folder = "dev.l1x.be"
    let localFolder = "tmp"

    match credentials with
    | Some creds ->

        let client = new AmazonS3Client(creds, config)
        let s3v2 = S3v2(client, loggerMain.LogInfo)
        let pattern = sprintf "^.*%s.*$" month
        let fileStates = new Dictionary<string, FileState>()

        whatToExecute startingState fileStates pattern s3v2 bucket s3folder localFolder month

        loggerMain.LogInfo <| sprintf "%A" fileStates

        // convert files to parquet
        // upload to partition
        do ()

    | None -> Environment.Exit 1

    0
