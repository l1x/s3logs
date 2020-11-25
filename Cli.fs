namespace S3logs

// internal
open Logging

// external
open System
open System.Text.RegularExpressions


module Cli =


    let loggerCli =
        Logger.CreateLogger "Cli" "info" (fun _ -> DateTime.Now)

    let isValidMonth s =
        Regex(@"^[0-9]{4}-(0[1-9]|1[1-2])$").Match(s).Success

    [<StructuredFormatDisplay("Month {Month}")>]
    type CommandLineOptions = { Month: string }


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

        // handle unrecognized option and keep looping
        | x :: xs ->
            loggerCli.LogError(String.Format("Option {0} is unrecognized", x))
            parseCommandLineRec xs optionsSoFar

    // create the "public" parse function
    let parseCommandLine args =
        // create the defaults
        let defaultOptions = { Month = "2020-01" }
        // call the recursive one with the initial options
        parseCommandLineRec args defaultOptions


// END
