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

  let FileStates = new Dictionary<string, FileState>()


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


  let downloadFile (s3v2: S3v2) (bucket: string) (key: string) =
    try
      let fileBytesMaybe = s3v2.GetS3ObjectBytes bucket key
      match fileBytesMaybe with
      | Ok (S3GetSuccess (k, v)) ->
          let fileName =
            sprintf "%s/%s" "tmp" (k.Replace('/', '_'))

          (new BinaryWriter(File.Open(fileName, FileMode.Create))).Write(v)
          Some key
      | Error err ->
          loggerMain.LogError <| sprintf "%A" err
          None
    with ex ->
      loggerMain.LogError <| sprintf "%A" ex.Message
      None


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
      AmazonS3Config(RegionEndpoint = region, Timeout = Nullable(TimeSpan.FromMilliseconds(500.0)))

    let bucket = "logs.l1x.be"
    let folder = "dev.l1x.be"

    match credentials with
    | Some creds ->

        let client = new AmazonS3Client(creds, config)
        let s3v2 = S3v2(client, loggerMain.LogInfo)
        let pattern = sprintf "^.*%s.*$" month

        let fileListMaybe =
          listFiles s3v2 bucket folder
          |> Option.map (List.filter (fun x -> Regex(pattern).Match(x).Success))
          |> Option.map (List.iter (fun f -> FileStates.Add(f, NotStarted)))

        loggerMain.LogInfo <| sprintf "%A" fileListMaybe

        // let downloadedFilesMaybe =
        //   match fileListMaybe with
        //   | Some x -> List.map (downloadFile s3v2 bucket) x
        //   | None -> []

        // let unzipFiles =
        //   downloadedFilesMaybe
        //   |> List.filter Option.isSome
        //   |> List.map (fun x -> x.Value)
        //   |> List.iter (fun y -> ZipFile.ExtractToDirectory(y, "tmp/"))

        // loggerMain.LogInfo <| sprintf "%A" unzipFiles


        // unzip files
        // merge files
        // convert files to parquet
        // upload to partition
        ()

    | None -> Environment.Exit 1

    0
