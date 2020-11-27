namespace S3logs

// internal
open Logging

// external
open System
open System.Text.RegularExpressions


module Cli =


  let loggerCli =
    Logger.CreateLogger "Cli" "info" (fun _ -> DateTime.Now)

  let fromStringToFileState s =
    match s with
    | "not-started" -> NotStarted
    | "downloaded" -> Downloaded
    | "unzipped" -> Unzipped
    | "merged" -> Merged
    | "converted" -> Converted
    | _ -> ParsingError

  let isValidMonth s =
    Regex(@"^[0-9]{4}\-(0?[1-9]|1[012])$").Match(s).Success


  let isValidAwsProfile s = Regex(@"^.*$").Match(s).Success


  let isValidAwsRegion s = Regex(@"^.*$").Match(s).Success


  [<StructuredFormatDisplay("Month: {Month} :: AwsProfile: {AwsProfile} :: AwsRegion: {AwsRegion} :: State: {State}")>]
  type CommandLineOptions =
    { Month: string
      AwsProfile: string
      AwsRegion: string
      State: FileState }


  // create the "helper" recursive function
  let rec private parseCommandLineRec args optionsSoFar =
    //loggerCli.LogInfo(args)
    match args with
    // empty list means we're done.
    | [] ->
        loggerCli.LogInfo(sprintf "optionsSoFar %A" optionsSoFar)
        optionsSoFar

    // match month
    | "--month" :: xs ->
        match xs with
        | month :: xss ->
            match isValidMonth month with
            | true -> parseCommandLineRec xss { optionsSoFar with Month = month }
            | false ->
                loggerCli.LogError(String.Format("Unsupported month: {0}", month))
                Environment.Exit 1
                parseCommandLineRec xss optionsSoFar // never reach

        | [] ->
            loggerCli.LogError(String.Format("Month cannot be empty"))
            Environment.Exit 1
            parseCommandLineRec xs optionsSoFar // never reach

    // match aws profile
    | "--aws-profile" :: xs ->
        match xs with
        | awsProfile :: xss ->
            match isValidAwsProfile awsProfile with
            | true ->
                parseCommandLineRec
                  xss
                  { optionsSoFar with
                      AwsProfile = awsProfile }
            | false ->
                loggerCli.LogError(String.Format("Unsupported awsProfile: {0}", awsProfile))
                Environment.Exit 1
                parseCommandLineRec xss optionsSoFar // never reach

        | [] ->
            loggerCli.LogError(String.Format("awsProfile cannot be empty"))
            Environment.Exit 1
            parseCommandLineRec xs optionsSoFar // never reach

    // match aws region
    | "--aws-region" :: xs ->
        match xs with
        | awsRegion :: xss ->
            match isValidAwsProfile awsRegion with
            | true ->
                parseCommandLineRec
                  xss
                  { optionsSoFar with
                      AwsRegion = awsRegion }
            | false ->
                loggerCli.LogError(String.Format("Unsupported awsRegion: {0}", awsRegion))
                Environment.Exit 1
                parseCommandLineRec xss optionsSoFar // never reach

        | [] ->
            loggerCli.LogError(String.Format("Month cannot be empty"))
            Environment.Exit 1
            parseCommandLineRec xs optionsSoFar // never reach

    // match state
    | "--state" :: xs ->
        match xs with
        | state :: xss ->
            match fromStringToFileState state with
            | NotStarted
            | Downloaded
            | Unzipped
            | Merged
            | Converted ->
                parseCommandLineRec
                  xss
                  { optionsSoFar with
                      State = (fromStringToFileState state) }
            | _ ->
                loggerCli.LogError(String.Format("Unsupported state: {0}", state))
                Environment.Exit 1
                parseCommandLineRec xss optionsSoFar // never reach

        | [] ->
            loggerCli.LogError(String.Format("State cannot be empty"))
            Environment.Exit 1
            parseCommandLineRec xs optionsSoFar // never reach


    // handle unrecognized option and keep looping
    | x :: xs ->
        loggerCli.LogError(String.Format("Option {0} is unrecognized", x))
        parseCommandLineRec xs optionsSoFar

  // create the "public" parse function
  let parseCommandLine args =
    // create the defaults
    let defaultOptions =
      { Month = "2020-01"
        AwsProfile = "not-really"
        AwsRegion = "eu-west-1"
        State = NotStarted }
    // call the recursive one with the initial options
    parseCommandLineRec args defaultOptions


// END
