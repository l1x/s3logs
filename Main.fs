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
open BenchmarkDotNet.Running


module Main =


  let loggerMain =
    Logger.CreateLogger "Main" "info" (fun () -> DateTime.Now)


  let rec listFilesInternal (acc) (maxKeys) (nextToken) (s3v2: S3v2) (bucket: string) (folder: string) =
    let fileListMaybe =
      s3v2.ListS3Objects maxKeys nextToken bucket folder

    match fileListMaybe with
    | Ok (S3ListSuccess (prefix, true, nextToken, l)) ->
        listFilesInternal (List.concat [ l; acc ]) maxKeys (Some nextToken) s3v2 bucket folder
    | Ok (S3ListSuccess (prefix, false, _, l)) ->
        Some(List.map (fun (x: Model.S3Object) -> x.Key) (List.concat [ l; acc ]))
    | Error err ->
        loggerMain.LogInfo <| sprintf "%A" err
        None


  let listFiles (s3v2: S3v2) (bucket: string) (folder: string) =
    loggerMain.LogInfo
    <| sprintf "%s %s" bucket folder
    listFilesInternal [] (Some 100) None s3v2 bucket folder


  let doPrintCount (fileStates: Dictionary<string, FileState>) =
    loggerMain.LogInfo
    <| sprintf "%A" (fileStates.Count)


  let doListFiles (fileStates: Dictionary<string, FileState>) pattern s3v2 bucket folder startingState =
    listFiles s3v2 bucket folder
    |> Option.map (List.filter (fun x -> Regex(pattern).Match(x).Success))
    |> Option.map (List.iter (fun f -> fileStates.Add(f, startingState)))
    |> Option.defaultWith (fun _ ->
         loggerMain.LogError "Could not list files"
         Environment.Exit 1)
    doPrintCount fileStates


  let downloadFile (s3v2: S3v2) (localFolder: string) (bucket: string) (key: string) =
    try
      loggerMain.LogInfo
      <| sprintf "Downloading: %s" key
      let fileBytesMaybe = s3v2.GetS3ObjectBytes bucket key
      match fileBytesMaybe with
      | Ok (S3GetBytesSuccess (k, v)) ->
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

    let asynTaskList =
      fileStates
      |> Seq.map (fun fileEntry ->
           async {
             match (downloadFile s3v2 localFolder bucket fileEntry.Key) with
             | Ok _x -> return (fileEntry.Key, Downloaded)
             | Error err -> return (fileEntry.Key, (FileStateError err))
           })

    Async.Parallel(asynTaskList, 10)
    |> Async.RunSynchronously
    |> Seq.iter (fun (k, v) -> fileStates.[k] <- v)


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
    Array.concat [| [| (sprintf "%sT%s" line.[0] line.[1]) |]
                    line.[2..] |]

  let doConvertFileToParquet localFolder month =
    let mergedFile = sprintf "%s/merged/%s" localFolder month

    let lines =
      File.ReadLines(mergedFile)
      |> Seq.filter (startsWithHash >> not)
      |> Seq.map (splitLineWithTab >> processLine)

    let parquetBytes = ParquetLogs.processLogRows lines
    File.WriteAllBytes((sprintf "%s.parquet" mergedFile), parquetBytes)

  let doUploadParquetFile (s3v2: S3v2) bucket localFolder month =
    let parquetFile =
      sprintf "%s/merged/%s.parquet" localFolder month

    let ret =
      s3v2.PutS3ObjectBytes
        bucket
        (sprintf "dwh/web-logs/month=%s/%s.parquet" month month)
        "application/octet-stream"
        (File.ReadAllBytes(parquetFile))

    match ret with
    | Ok (S3PutSuccess k) ->
        loggerMain.LogInfo
        <| sprintf "Successfully uploaded Parquet file: %s" k
    | err ->
        loggerMain.LogError
        <| sprintf "Uploading Parquet file has failed: %A" err


  let whatToExecute state fileStates pattern s3v2 bucket s3folder localFolder month =
    match state with
    | NotStarted ->
        doListFiles fileStates pattern s3v2 bucket s3folder state
        doDownloadFiles fileStates s3v2 localFolder bucket
        doUnzipFiles fileStates localFolder
        doMergeFiles fileStates month localFolder
        doConvertFileToParquet localFolder month
        doUploadParquetFile s3v2 bucket localFolder month
    | Downloaded ->
        doListFiles fileStates pattern s3v2 bucket s3folder state
        doUnzipFiles fileStates localFolder
        doMergeFiles fileStates month localFolder
        doConvertFileToParquet localFolder month
        doUploadParquetFile s3v2 bucket localFolder month
    | Unzipped ->
        doListFiles fileStates pattern s3v2 bucket s3folder state
        doMergeFiles fileStates month localFolder
        doConvertFileToParquet localFolder month
        doUploadParquetFile s3v2 bucket localFolder month
    | Merged ->
        doConvertFileToParquet localFolder month
        doUploadParquetFile s3v2 bucket localFolder month
    | Converted -> doUploadParquetFile s3v2 bucket localFolder month
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
      AmazonS3Config(RegionEndpoint = region, Timeout = Nullable(TimeSpan.FromMilliseconds(300000.0)))

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

    // BenchmarkRunner.Run<Bm.LengthBench>() |> ignore

    0
